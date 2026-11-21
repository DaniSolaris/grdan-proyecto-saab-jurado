"""Serie diaria de intensidad de los sensores seleccionados, en formato largo.

Parte del bruto en Parquet (`descarga_bruto.py`) y de la lista de sensores que
elige el notebook 01 (`data/seleccion/sensores_200.csv`). Devuelve una fila por
sensor y día con las columnas que espera Nixtla (`unique_id`, `ds`, `y`).

Pasos:
  1. Resume cada sensor-día (nº de lecturas, ceros, mínimo, máximo y media).
  2. Clasifica cada sensor-día con las mismas reglas del notebook 01
     (sin datos, todo a cero, incompleto, ceros de día, valor congelado).
  3. `y` es la intensidad media del día en veh/h, solo en los días correctos.
  4. Los días con fallo se imputan con el último valor válido del mismo día
     de la semana. Solo mira hacia atrás, así que no introduce información
     del futuro en ningún corte temporal.

La semana de test se construye con las mismas reglas, pero sus días con fallo
NO se imputan: quedan marcados para excluirlos de la evaluación.

Uso desde un notebook:
    from serie_diaria import construir, partir
    serie = construir()
    train, test = partir(serie)
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
BRUTO = RAIZ / "data" / "bruto"
SELECCION = RAIZ / "data" / "seleccion" / "sensores_200.csv"

INICIO = pd.Timestamp("2024-11-01")
FIN = pd.Timestamp("2026-10-31")
DIAS_TEST = 7  # horizonte del encargo
TEST_INICIO = FIN - pd.Timedelta(days=DIAS_TEST - 1)

INTENSIDAD_MAX = 15_000  # veh/h; por encima es un error de equipo
MIN_COMPLETO = 0.75      # parte mínima de los cuartos de hora para dar el día por bueno
CAIDA_GENERAL = 0.5      # si falla más de esta parte de los sensores, es una caída de la red

ESTADOS = ["sin datos", "sin lecturas válidas", "todo a cero", "incompleto", "ceros de día", "valor congelado"]


def cargar_seleccion(ruta: Path = SELECCION) -> pd.DataFrame:
    """Sensores elegidos en el notebook 01, con su distrito, nombre y coordenadas."""
    if not Path(ruta).exists():
        raise FileNotFoundError(f"Falta {ruta}. Ejecuta antes el notebook 01_eda_calidad_y_seleccion.")
    return pd.read_csv(ruta, index_col="id")


def control_diario(ids, bruto: Path = BRUTO) -> pd.DataFrame:
    """Una fila por sensor y día con los contadores de calidad y la intensidad media."""
    ficheros = (Path(bruto) / "trafico" / "*.parquet").as_posix()
    lista = ", ".join(str(int(i)) for i in ids)
    control = duckdb.sql(f"""
        SELECT id,
               CAST(fecha AS DATE) AS ds,
               count(*)::INT AS n_lect,
               sum((intensidad > {INTENSIDAD_MAX})::INT)::INT AS n_imposible,
               sum((intensidad = 0)::INT)::INT AS n_cero,
               sum((hour(fecha) BETWEEN 7 AND 21)::INT)::INT AS n_lect_dia,
               sum((intensidad = 0 AND hour(fecha) BETWEEN 7 AND 21)::INT)::INT AS n_cero_dia,
               min(intensidad) FILTER (WHERE intensidad <= {INTENSIDAD_MAX}) AS int_min,
               max(intensidad) FILTER (WHERE intensidad <= {INTENSIDAD_MAX}) AS int_max,
               avg(intensidad) FILTER (WHERE intensidad <= {INTENSIDAD_MAX}) AS int_media
        FROM read_parquet('{ficheros}')
        WHERE tipo_elem = 'URB' AND id IN ({lista})
        GROUP BY ALL
    """).df()
    control["ds"] = pd.to_datetime(control["ds"])
    control["n_valid"] = control["n_lect"] - control["n_imposible"]
    return control


def ceros_mensuales(ids, bruto: Path = BRUTO) -> pd.DataFrame:
    """Por mes, en entrenamiento: % de lecturas a cero e intensidad media con y sin ceros."""
    ficheros = (Path(bruto) / "trafico" / "*.parquet").as_posix()
    lista = ", ".join(str(int(i)) for i in ids)
    return duckdb.sql(f"""
        SELECT date_trunc('month', fecha) AS mes,
               avg((intensidad = 0)::INT) * 100 AS pct_ceros,
               avg(intensidad) FILTER (WHERE intensidad <= {INTENSIDAD_MAX}) AS media,
               avg(intensidad) FILTER (WHERE intensidad BETWEEN 1 AND {INTENSIDAD_MAX}) AS media_sin_ceros
        FROM read_parquet('{ficheros}')
        WHERE tipo_elem = 'URB' AND id IN ({lista}) AND fecha < TIMESTAMP '{TEST_INICIO:%Y-%m-%d}'
        GROUP BY 1 ORDER BY 1
    """).df().set_index("mes")


def _cuartos_esperados(dias: pd.DatetimeIndex) -> pd.Series:
    """96 por día, salvo el día de 23 horas de marzo, en el que la fuente entrega 93."""
    madrid = pd.date_range(dias[0], dias[-1] + pd.Timedelta(days=1), freq="D", tz="Europe/Madrid")
    horas = pd.Series((madrid[1:] - madrid[:-1]) / pd.Timedelta(hours=1), index=dias)
    return pd.Series(np.where(horas < 24, 93, 96), index=dias)


def clasificar(control: pd.DataFrame, ids) -> pd.DataFrame:
    """Rejilla completa sensor × día con el estado de cada celda (reglas del notebook 01)."""
    dias = pd.date_range(INICIO, FIN, freq="D")
    rejilla = pd.MultiIndex.from_product([sorted(ids), dias], names=["id", "ds"])
    p = control.set_index(["id", "ds"]).reindex(rejilla).reset_index()
    p["esperado"] = p["ds"].map(_cuartos_esperados(dias))
    reglas = [
        p["n_lect"].isna(),
        p["n_valid"].eq(0),
        p["n_cero"].eq(p["n_valid"]),
        p["n_valid"] < MIN_COMPLETO * p["esperado"],
        (p["n_lect_dia"] > 0) & (p["n_cero_dia"] >= 0.5 * p["n_lect_dia"]),
        p["int_max"].eq(p["int_min"]),
    ]
    reglas = [r.fillna(False).to_numpy(dtype=bool) for r in reglas]
    p["estado"] = np.select(reglas, ESTADOS, default="correcto")
    fallo = p["estado"] != "correcto"
    p["caida_general"] = p["ds"].map(fallo.groupby(p["ds"]).mean() > CAIDA_GENERAL)
    return p


def _imputar_hacia_atras(y: pd.Series, claves: pd.DataFrame) -> pd.Series:
    """Rellena cada hueco con el último valor válido del mismo sensor y día de la semana.

    Si el hueco está al principio y no hay valor anterior, toma el primero posterior.
    """
    grupos = [claves["unique_id"], claves["ds"].dt.day_of_week]
    relleno = y.groupby(grupos).ffill()
    return relleno.fillna(y.groupby(grupos).bfill())


def construir(ids=None, bruto: Path = BRUTO) -> pd.DataFrame:
    """Serie diaria en formato largo, de INICIO a FIN, con la semana de test incluida.

    Columnas:
      unique_id, ds   identificador del sensor y día
      y               intensidad media diaria (veh/h); imputada si el día tiene fallo
                      en entrenamiento, y nula si lo tiene en la semana de test
      y_observada     lo mismo sin imputar (nula en los días con fallo)
      estado          estado del sensor-día
      caida_general   True si ese día falla más de la mitad de los sensores
      imputado        True si `y` no es un valor observado
    """
    if ids is None:
        ids = cargar_seleccion().index
    p = clasificar(control_diario(ids, bruto), ids)
    valido = (p["estado"] == "correcto") & ~p["caida_general"]
    serie = pd.DataFrame({
        "unique_id": p["id"].astype(str),
        "ds": p["ds"],
        "y_observada": p["int_media"].where(valido),
        "estado": p["estado"],
        "caida_general": p["caida_general"],
    }).sort_values(["unique_id", "ds"], ignore_index=True)
    serie["imputado"] = serie["y_observada"].isna()

    es_test = serie["ds"] >= TEST_INICIO
    # La imputación se hace solo con entrenamiento: la semana de test no interviene
    y = serie["y_observada"].where(~es_test)
    y = _imputar_hacia_atras(y, serie[["unique_id", "ds"]])
    serie.insert(2, "y", y.where(~es_test, serie["y_observada"]))
    return serie


def partir(serie: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Entrenamiento (todo lo anterior a la semana de test) y test (los últimos 7 días)."""
    es_test = serie["ds"] >= TEST_INICIO
    return serie[~es_test].reset_index(drop=True), serie[es_test].reset_index(drop=True)


def total(serie: pd.DataFrame) -> pd.DataFrame:
    """Serie agregada: suma de `y` de los sensores cada día (unique_id = 'TOTAL').

    Solo suma los sensores que no tienen ningún nulo en `serie`, para que todos los
    días sumen el mismo conjunto de sensores y sean comparables.
    """
    nulos = serie["y"].isna().groupby(serie["unique_id"]).sum()
    completos = nulos.index[nulos == 0]
    t = serie[serie["unique_id"].isin(completos)].groupby("ds")["y"].sum().reset_index()
    t.insert(0, "unique_id", "TOTAL")
    t.attrs["sensores"] = len(completos)
    return t


def muestra(train: pd.DataFrame, n: int = 10) -> list[str]:
    """Muestra de `n` sensores repartidos por nivel de tráfico.

    Ordena los sensores por su intensidad media en entrenamiento y toma uno de cada
    tramo (percentiles 5, 15, ..., 95 si n = 10), para cubrir desde calles de barrio
    hasta grandes ejes sin elegir a mano.
    """
    orden = train.groupby("unique_id")["y"].mean().sort_values()
    posiciones = [int(round(q * (len(orden) - 1))) for q in np.linspace(0.5 / n, 1 - 0.5 / n, n)]
    return orden.index[posiciones].tolist()


# Festivos de Madrid capital dentro del periodo. Lista manual, contrastada con las caídas
# de tráfico que se ven en los datos (notebook 02); en E2 se tomará de una fuente oficial.
FESTIVOS = {
    "2024-11-01": "Todos los Santos", "2024-12-06": "Constitución", "2024-12-25": "Navidad",
    "2025-01-01": "Año Nuevo", "2025-01-06": "Reyes", "2025-04-17": "Jueves Santo", "2025-04-18": "Viernes Santo",
    "2025-05-01": "Día del Trabajo", "2025-05-02": "Comunidad de Madrid", "2025-05-15": "San Isidro",
    "2025-07-25": "Santiago Apóstol", "2025-08-15": "Asunción", "2025-11-10": "Almudena (trasladada)",
    "2025-12-08": "Inmaculada", "2025-12-25": "Navidad",
    "2026-01-01": "Año Nuevo", "2026-01-06": "Reyes", "2026-04-02": "Jueves Santo", "2026-04-03": "Viernes Santo",
    "2026-05-01": "Día del Trabajo", "2026-05-15": "San Isidro", "2026-10-12": "Fiesta Nacional",
}

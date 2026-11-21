"""Descarga y reducción a diario del histórico de tráfico de Madrid.

Fuente: Ayuntamiento de Madrid, Portal de Datos Abiertos (CC BY 4.0).
  - Histórico de datos del tráfico desde 2013 (dataset 208627)
  - Ubicación de los puntos de medida del tráfico (dataset 202468)

Cada mes es un ZIP de ~95 MB con un CSV de ~800 MB (13-14 M de filas, una por
punto y cuarto de hora). No se carga entero: se lee por trozos, se filtra a
puntos urbanos y se agrega a punto-día. Solo se guarda ese resumen en parquet.
"""
from __future__ import annotations

import re
import zipfile
from pathlib import Path

import pandas as pd
import requests

RDF_HISTORICO = "https://datos.madrid.es/dataset/208627-0-transporte-ptomedida-historico.rdf"
RDF_UBICACION = "https://datos.madrid.es/dataset/202468-0-intensidad-trafico.rdf"
CABECERAS = {"User-Agent": "Mozilla/5.0"}

MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11,
    "diciembre": 12,
}
# En ficheros antiguos el tipo urbano aparece con el nombre largo.
TIPOS_URBANOS = {"URB", "PUNTOS MEDIDA URBANOS"}
# Una lectura por encima de este valor (veh/h en un punto urbano) es un error
# de equipo: el percentil 99,99 real ronda los 5.000 veh/h.
INTENSIDAD_MAX_PLAUSIBLE = 15_000

_PATRON_DISTRIBUCION = re.compile(
    r"<dct:description[^>]*>([^<]*)</dct:description>.*?"
    r"<dcat:accessURL rdf:resource=\"([^\"]+)\"/>.*?"
    r"<dcat:byteSize>(\d+)</dcat:byteSize>",
    re.S,
)
_PATRON_MES = re.compile(r"(%s)\s+(?:de\s+)?(\d{4})" % "|".join(MESES), re.I)


def catalogo(url_rdf: str) -> pd.DataFrame:
    """Lee el catálogo RDF del portal y devuelve un fichero por fila.

    Las URL de descarga no siguen un patrón por fecha, así que se obtienen del
    catálogo en lugar de escribirlas a mano.
    """
    texto = requests.get(url_rdf, headers=CABECERAS, timeout=60).text
    filas = []
    for bloque in texto.split("<dcat:Distribution ")[1:]:
        m = _PATRON_DISTRIBUCION.search(bloque)
        if not m:
            continue
        descripcion, url, tam = m.groups()
        mes = _PATRON_MES.search(descripcion)
        if not mes:
            continue  # documentación asociada (PDF), no es un mes de datos
        periodo = pd.Period(year=int(mes.group(2)), month=MESES[mes.group(1).lower()], freq="M")
        filas.append(
            {"mes": periodo, "formato": url.rsplit(".", 1)[-1].lower(), "url": url, "bytes": int(tam)}
        )
    return pd.DataFrame(filas).sort_values(["mes", "formato"]).reset_index(drop=True)


def descargar(url: str, destino: Path) -> Path:
    """Descarga en streaming a un fichero temporal y lo renombra al terminar."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    if destino.exists():
        return destino
    tmp = destino.with_suffix(destino.suffix + ".part")
    with requests.get(url, headers=CABECERAS, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for trozo in r.iter_content(chunk_size=1 << 20):
                f.write(trozo)
    tmp.rename(destino)
    return destino


def reducir_mes(ruta_zip: Path, tam_trozo: int = 1_000_000) -> pd.DataFrame:
    """Reduce el CSV mensual a una fila por punto urbano y día.

    Columnas devueltas:
      id, ds          punto de medida y fecha
      n_lect          lecturas de 15 min recibidas (96 si el día está completo)
      n_valid         lecturas válidas (sin error y con valor plausible)
      n_cero          lecturas válidas con intensidad 0
      int_media       intensidad media de las lecturas válidas (veh/h)
      int_max         intensidad máxima válida (veh/h)
    """
    partes = []
    with zipfile.ZipFile(ruta_zip) as z:
        nombre = max(z.infolist(), key=lambda i: i.file_size).filename
        with z.open(nombre) as f:
            lector = pd.read_csv(
                f,
                sep=";",
                chunksize=tam_trozo,
                usecols=["id", "fecha", "tipo_elem", "intensidad", "error"],
                dtype={"id": "int32", "fecha": "string", "tipo_elem": "string", "error": "string"},
                encoding="latin-1",
            )
            for trozo in lector:
                trozo = trozo[trozo["tipo_elem"].str.strip().isin(TIPOS_URBANOS)]
                if trozo.empty:
                    continue
                valida = (
                    trozo["error"].fillna("N").eq("N")
                    & trozo["intensidad"].between(0, INTENSIDAD_MAX_PLAUSIBLE)
                )
                trozo = trozo.assign(
                    ds=trozo["fecha"].str.slice(0, 10),
                    valida=valida,
                    cero=valida & trozo["intensidad"].eq(0),
                    int_valida=trozo["intensidad"].where(valida),
                )
                partes.append(
                    trozo.groupby(["id", "ds"], sort=False).agg(
                        n_lect=("intensidad", "size"),
                        n_valid=("valida", "sum"),
                        n_cero=("cero", "sum"),
                        suma=("int_valida", "sum"),
                        int_max=("int_valida", "max"),
                    )
                )
    # Un punto-día puede caer en dos trozos: se vuelve a agregar.
    diario = (
        pd.concat(partes)
        .groupby(level=["id", "ds"])
        .agg({"n_lect": "sum", "n_valid": "sum", "n_cero": "sum", "suma": "sum", "int_max": "max"})
        .reset_index()
    )
    diario["ds"] = pd.to_datetime(diario["ds"], format="%Y-%m-%d")
    diario["int_media"] = diario["suma"] / diario["n_valid"].where(diario["n_valid"] > 0)
    return diario.drop(columns="suma").astype(
        {"n_lect": "int16", "n_valid": "int16", "n_cero": "int16"}
    )


def construir_diario(
    desde: str, hasta: str, carpeta: Path, conservar_zip: bool = False, verbose: bool = True
) -> pd.DataFrame:
    """Descarga y reduce los meses [desde, hasta] (formato 'AAAA-MM').

    Cada mes reducido se guarda en `carpeta/diario/AAAA-MM.parquet`; si ya
    existe no se vuelve a descargar. El ZIP se borra tras reducirlo salvo que
    se pida conservarlo.
    """
    carpeta = Path(carpeta)
    (carpeta / "diario").mkdir(parents=True, exist_ok=True)
    meses = pd.period_range(desde, hasta, freq="M")
    pendientes = [m for m in meses if not (carpeta / "diario" / f"{m}.parquet").exists()]
    if pendientes:
        cat = catalogo(RDF_HISTORICO)
        cat = cat[cat["formato"] == "zip"].set_index("mes")
        faltan = [str(m) for m in pendientes if m not in cat.index]
        if faltan:
            raise ValueError(f"Meses sin publicar en el portal: {faltan}")
        for m in pendientes:
            ruta_zip = descargar(cat.loc[m, "url"], carpeta / "raw" / f"{m}.zip")
            diario = reducir_mes(ruta_zip)
            # Algún fichero trae lecturas sueltas de meses vecinos: se descartan.
            diario = diario[diario["ds"].dt.to_period("M") == m]
            diario.to_parquet(carpeta / "diario" / f"{m}.parquet", index=False)
            if not conservar_zip:
                ruta_zip.unlink()
            if verbose:
                print(f"{m}: {len(diario):>7,} punto-día, {diario['id'].nunique():,} puntos urbanos")
    return pd.concat(
        [pd.read_parquet(carpeta / "diario" / f"{m}.parquet") for m in meses], ignore_index=True
    )


def construir_ubicacion(desde: str, hasta: str, carpeta: Path) -> pd.DataFrame:
    """Descarga el fichero de ubicación de cada mes y los apila con su mes.

    Sirve para saber en qué meses existe cada punto (altas y bajas) y para
    asignarle distrito, nombre y coordenadas.
    """
    carpeta = Path(carpeta) / "ubicacion"
    cat = catalogo(RDF_UBICACION)
    cat = cat[cat["formato"] == "csv"].drop_duplicates("mes").set_index("mes")
    tablas = []
    for m in pd.period_range(desde, hasta, freq="M"):
        if m not in cat.index:
            continue  # el notebook informa de los meses sin fichero
        ruta = descargar(cat.loc[m, "url"], carpeta / f"{m}.csv")
        t = pd.read_csv(ruta, sep=";", encoding="latin-1")
        t.columns = [c.strip().strip('"').lower() for c in t.columns]
        t["mes"] = m
        tablas.append(t)
    return pd.concat(tablas, ignore_index=True)

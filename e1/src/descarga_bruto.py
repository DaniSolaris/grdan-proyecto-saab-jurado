"""Descarga del histórico de tráfico de Madrid en bruto, convertido a Parquet.

Fuente: Ayuntamiento de Madrid, Portal de Datos Abiertos (CC BY 4.0).
  - Histórico de datos del tráfico desde 2013 (dataset 208627): un ZIP por mes
    con un CSV de ~800 MB (una fila por punto de medida y cuarto de hora).
  - Ubicación de los puntos de medida del tráfico (dataset 202468): un CSV por mes.

No se filtra ni se agrega nada: mismas filas, mismas columnas y mismos valores
que el CSV original. La única transformación es el formato (CSV -> Parquet),
que obliga a dar un tipo a cada columna: fecha como timestamp, números como
enteros y textos como string. Si algún mes trae valores que no encajan en esos
tipos, ese mes se guarda con todas las columnas como texto y se avisa.

El CSV se convierte en streaming, por bloques, así que no hace falta cargarlo
entero en memoria. Los ZIP se borran tras convertirlos salvo `--conservar-zip`.

Uso:
    uv run python e1/src/descarga_bruto.py                      # nov-2024 a oct-2026
    uv run python e1/src/descarga_bruto.py --desde 2025-01 --hasta 2025-03
    uv run python e1/src/descarga_bruto.py --conservar-zip

Resultado (en `e1/data/bruto/`, ignorado por Git):
    trafico/AAAA-MM.parquet     ~14 M de filas por mes
    ubicacion/AAAA-MM.parquet   ~4.800 puntos por mes
"""
from __future__ import annotations

import argparse
import re
import time
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import requests

RDF_HISTORICO = "https://datos.madrid.es/dataset/208627-0-transporte-ptomedida-historico.rdf"
RDF_UBICACION = "https://datos.madrid.es/dataset/202468-0-intensidad-trafico.rdf"
CABECERAS = {"User-Agent": "Mozilla/5.0"}

MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11,
    "diciembre": 12,
}
_PATRON_DISTRIBUCION = re.compile(
    r"<dct:description[^>]*>([^<]*)</dct:description>.*?"
    r"<dcat:accessURL rdf:resource=\"([^\"]+)\"/>.*?"
    r"<dcat:byteSize>(\d+)</dcat:byteSize>",
    re.S,
)
_PATRON_MES = re.compile(r"(%s)\s+(?:de\s+)?(\d{4})" % "|".join(MESES), re.I)

CARPETA = Path(__file__).resolve().parents[1] / "data" / "bruto"
DESDE, HASTA = "2024-11", "2026-10"

# Columnas del CSV de tráfico tal como vienen en la cabecera
TIPOS_TRAFICO = {
    "id": pa.int32(),
    "fecha": pa.timestamp("s"),
    "tipo_elem": pa.string(),
    "intensidad": pa.int32(),
    "ocupacion": pa.int32(),
    "carga": pa.int32(),
    "vmed": pa.int32(),
    "error": pa.string(),
    "periodo_integracion": pa.int32(),
}


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


def _csv_a_parquet(abrir, destino: Path, tipos: dict | None) -> int:
    """Convierte un CSV de la fuente a Parquet por bloques. Devuelve nº de filas.

    `tipos=None` deja que pyarrow deduzca los tipos (solo para ficheros pequeños,
    que caben en un bloque y se deducen de una vez).
    """
    tmp = destino.with_suffix(".parquet.part")
    filas = 0
    with abrir() as f:
        lector = pv.open_csv(
            f,
            read_options=pv.ReadOptions(encoding="latin-1", block_size=64 << 20),
            parse_options=pv.ParseOptions(delimiter=";"),
            convert_options=pv.ConvertOptions(column_types=tipos or {}, strings_can_be_null=False),
        )
        with pq.ParquetWriter(tmp, lector.schema, compression="zstd") as w:
            for bloque in lector:
                w.write_batch(bloque)
                filas += bloque.num_rows
    tmp.replace(destino)
    return filas


def convertir_trafico(ruta_zip: Path, destino: Path) -> int:
    """Pasa el CSV del ZIP mensual a Parquet sin tocar filas ni valores."""
    with zipfile.ZipFile(ruta_zip) as z:
        nombre = max(z.infolist(), key=lambda i: i.file_size).filename
        abrir = lambda: z.open(nombre)  # noqa: E731
        try:
            return _csv_a_parquet(abrir, destino, TIPOS_TRAFICO)
        except pa.ArrowInvalid as e:
            destino.with_suffix(".parquet.part").unlink(missing_ok=True)
            print(f"  aviso: {destino.stem} no encaja en los tipos ({e}). Se guarda como texto.")
            return _csv_a_parquet(abrir, destino, {c: pa.string() for c in TIPOS_TRAFICO})


def descargar_trafico(desde: str, hasta: str, carpeta: Path = CARPETA, conservar_zip: bool = False) -> None:
    """Descarga y convierte a Parquet los meses [desde, hasta] (formato 'AAAA-MM')."""
    salida = carpeta / "trafico"
    salida.mkdir(parents=True, exist_ok=True)
    meses = pd.period_range(desde, hasta, freq="M")
    pendientes = [m for m in meses if not (salida / f"{m}.parquet").exists()]
    print(f"Tráfico: {len(meses)} meses, {len(meses) - len(pendientes)} ya convertidos.")
    if not pendientes:
        return
    cat = catalogo(RDF_HISTORICO)
    cat = cat[cat["formato"] == "zip"].set_index("mes")
    faltan = [str(m) for m in pendientes if m not in cat.index]
    if faltan:
        raise ValueError(f"Meses sin publicar en el portal: {faltan}")
    for m in pendientes:
        t0 = time.time()
        ruta_zip = descargar(cat.loc[m, "url"], carpeta / "zip" / f"{m}.zip")
        filas = convertir_trafico(ruta_zip, salida / f"{m}.parquet")
        if not conservar_zip:
            ruta_zip.unlink()
        mb = (salida / f"{m}.parquet").stat().st_size / 1e6
        print(f"  {m}: {filas:>11,} filas · {mb:,.0f} MB · {time.time() - t0:,.0f} s", flush=True)


def descargar_ubicacion(desde: str, hasta: str, carpeta: Path = CARPETA) -> None:
    """Descarga el CSV de ubicación de cada mes y lo guarda en Parquet tal cual."""
    salida = carpeta / "ubicacion"
    salida.mkdir(parents=True, exist_ok=True)
    cat = catalogo(RDF_UBICACION)
    cat = cat[cat["formato"] == "csv"].drop_duplicates("mes").set_index("mes")
    sin_fichero = []
    for m in pd.period_range(desde, hasta, freq="M"):
        destino = salida / f"{m}.parquet"
        if destino.exists():
            continue
        if m not in cat.index:
            sin_fichero.append(str(m))
            continue
        ruta_csv = descargar(cat.loc[m, "url"], carpeta / "zip" / f"ubicacion-{m}.csv")
        _csv_a_parquet(lambda: open(ruta_csv, "rb"), destino, tipos=None)
        ruta_csv.unlink()
    print(f"Ubicación: hecho. Meses sin fichero en el portal: {sin_fichero or 'ninguno'}")


def leer_trafico(
    carpeta: Path = CARPETA,
    columnas: list[str] | None = None,
    ids: list[int] | None = None,
    desde: str | None = None,
    hasta: str | None = None,
) -> pd.DataFrame:
    """Lee el bruto de tráfico filtrando al leer (no carga lo que no se pide).

    Los 24 meses completos son ~330 M de filas y no caben en memoria: conviene
    pedir solo las columnas, los sensores o las fechas necesarias.
    `desde`/`hasta` son fechas 'AAAA-MM-DD' (hasta excluido).
    """
    dataset = ds.dataset(Path(carpeta) / "trafico", format="parquet")
    filtro = None
    if ids is not None:
        filtro = ds.field("id").isin(list(ids))
    if desde is not None:
        f = ds.field("fecha") >= pd.Timestamp(desde).to_pydatetime()
        filtro = f if filtro is None else filtro & f
    if hasta is not None:
        f = ds.field("fecha") < pd.Timestamp(hasta).to_pydatetime()
        filtro = f if filtro is None else filtro & f
    return dataset.to_table(columns=columnas, filter=filtro).to_pandas()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--desde", default=DESDE, help="primer mes, AAAA-MM (por defecto %(default)s)")
    p.add_argument("--hasta", default=HASTA, help="último mes, AAAA-MM (por defecto %(default)s)")
    p.add_argument("--carpeta", type=Path, default=CARPETA, help="carpeta de salida")
    p.add_argument("--conservar-zip", action="store_true", help="no borrar los ZIP tras convertirlos")
    a = p.parse_args()
    descargar_ubicacion(a.desde, a.hasta, a.carpeta)
    descargar_trafico(a.desde, a.hasta, a.carpeta, a.conservar_zip)

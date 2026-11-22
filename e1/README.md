# E1 · Tráfico de Madrid (hueco 4)

**Integrantes:** Daniel Saab y Carlos Jurado · Análisis Avanzado de Series Temporales · GRDAN 2627

**Encargo.** Planificación de obras en vía urbana: prever la intensidad diaria de tráfico a 7 días en unos 200 puntos
urbanos de Madrid, con un máximo de 2 años de histórico, para elegir las ventanas de obra con menos tráfico.

## Ficha de la fuente

- **Quién la publica:** Ayuntamiento de Madrid, Portal de Datos Abiertos. Dos conjuntos: *Tráfico. Histórico de datos
  del tráfico desde 2013* (dataset 208627) y *Ubicación de los puntos de medida del tráfico* (dataset 202468).
- **Frecuencia:** una lectura cada 15 minutos por punto de medida. Aquí se agrega a **diaria**.
- **Cobertura:** desde 2013, unos 5.000 puntos de medida de la ciudad (urbanos, M-30 y un tercer grupo etiquetado `C30`). Se usan los 24 meses
  de noviembre de 2024 a octubre de 2026 y 200 puntos urbanos.
- **Unidad:** intensidad en **vehículos/hora**. La serie diaria es la intensidad media del día en veh/h.
- **Retraso de actualización:** se publica un fichero por mes, una vez cerrado el mes, con unas semanas de retraso. No
  sirve para seguir el tráfico en tiempo real.
- **Licencia:** CC BY 4.0.
- **Limitaciones:** los fallos no vienen marcados. La columna `error` vale siempre `N` en los puntos urbanos, y los
  sensores caídos aparecen como filas que faltan o como ceros. Hay lecturas imposibles (hasta 99.999 veh/h), días en
  que cae toda la red a la vez, puntos que se dan de alta y de baja, y los puntos urbanos no miden velocidad. El
  fichero de ubicación de julio de 2026 trae las coordenadas mal formateadas.

## Qué hay en esta carpeta

| Fichero | Qué hace |
|---|---|
| `src/descarga_bruto.py` | Descarga los ficheros mensuales desde el portal y los pasa a Parquet sin filtrar ni agregar |
| `src/serie_diaria.py` | Construye la serie diaria en formato largo (`unique_id`, `ds`, `y`) de los 200 sensores |
| `notebooks/01_eda_calidad_y_seleccion.ipynb` | Calidad del bruto, fallos de sensores y selección de los 200 puntos (apartado A) |
| `notebooks/02_serie_diaria_y_patrones.ipynb` | Serie diaria, estacionalidad, festivos, ACF, descomposición y rupturas (apartados A y B) |
| `notebooks/03_particion_y_baselines.ipynb` | Partición, ventana rodante, baselines, métricas, residuos y plan para E2 (apartados C, D y E) |

Los datos no se versionan: se descargan desde código y quedan en `e1/data/`, ignorada por Git.

## Cómo reproducirlo

```bash
uv sync                                    # instala el entorno
uv run python e1/src/descarga_bruto.py     # descarga los 24 meses (~2,2 GB de descarga, ~1 GB en disco)
```

Después se ejecutan los notebooks **en orden** (01, 02 y 03): el 01 genera la lista de los 200 sensores que usan los
otros dos.

## Trampas encontradas y qué se decidió

| Trampa | Decisión |
|---|---|
| **Cada mes son ~780 MB de CSV y no cabe leerlo entero** | El CSV se convierte a Parquet por bloques, sin cargarlo en memoria. Se consulta con DuckDB y a pandas solo llega lo ya resumido |
| **Puntos que se dan de alta y de baja** | Se cruza con el fichero mensual de ubicación (131 altas y 30 bajas entre los urbanos). Solo entran sensores presentes en todas las fichas y con datos de principio a fin |
| **Ceros por sensor caído ≠ calle vacía** | Un día todo a cero, o con al menos la mitad de ceros entre las 7 y las 22 h, se trata como fallo y se imputa. Los ceros de madrugada se respetan |
| Lecturas imposibles | Las de más de 15.000 veh/h no entran en la media diaria |
| Días incompletos | Si llega menos del 75 % de los cuartos de hora, el día se imputa |
| Caídas generales de la red (8 días, entre ellos el apagón del 28-30 de abril de 2025) | Se imputan en todos los sensores |
| Enero de 2026: la fuente rellena huecos con ceros | Lo detectan las reglas de ceros; se comprueba además que no explica la bajada de tráfico de esos meses |

**Imputación.** Cada día con fallo se rellena con el último valor válido del mismo sensor y mismo día de la semana.
Solo mira hacia atrás, así que no introduce información del futuro. Afecta al 1,4 % de los sensor-días.

## Resultados principales

- **Patrones.** Ciclo semanal dominante (sábado −23 %, domingo −31 %), agosto un tercio por debajo del resto del año,
  y caídas de hasta el 50 % en festivos.
- **Partición.** Prueba: 25–31 de octubre de 2026 (7 días, un horizonte). Validación: ventana rodante de 52 orígenes
  semanales. En E2 los modelos se elegirán con la ventana rodante y la semana de prueba se mirará una sola vez.
- **Rival a batir.** *Seasonal naïve* con `season_length = 7`: MASE de 0,95 en la ventana rodante sobre el total, y el
  mejor de los cuatro baselines en 195 de los 200 sensores.
- **Dónde falla.** En las semanas con festivo y en las transiciones de vacaciones: el 6 % de los días concentra el
  35 % de su error.

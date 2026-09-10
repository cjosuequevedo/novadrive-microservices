# Databricks notebook source
# MAGIC %md
# MAGIC # 01b - Bronze: un pipeline, 3 procesos ETL (uno por tipo de evento)
# MAGIC
# MAGIC **Qué hace este notebook, en una frase:** lee cada uno de los 3 Volumes
# MAGIC fuente (`raw_events_customer`, `raw_events_vehicle`, `raw_events_sale`)
# MAGIC con Auto Loader, y materializa cada uno en su propia tabla Delta de Bronze
# MAGIC (`bronze.customer`, `bronze.vehicle`, `bronze.sale`).
# MAGIC
# MAGIC **Por qué NO hay un `if entity == "customer"` en este notebook.** La
# MAGIC clasificación por origen ya pasó río arriba: `bridge/bridge_redpanda_to_databricks.py`
# MAGIC (en el repo de la app, no acá) ya separó los eventos en 3 Volumes
# MAGIC distintos según el campo `entity` del payload, ANTES de que lleguen a
# MAGIC Databricks (`group_by_volume()`, Fase 15 de CLAUDE.md). Acá no hace falta
# MAGIC re-inspeccionar cada registro para decidir a dónde va - cada uno de los 3
# MAGIC procesos de este notebook simplemente lee el Volume que YA le corresponde.
# MAGIC
# MAGIC **Por qué NO hay nombres de tabla "consistentes con Silver" acá.** A
# MAGIC diferencia de Andes (`bronze.clientes` calza con `silver.clientes`,
# MAGIC que ya existía), **NovaDrive no tiene Silver** - está fuera de alcance a
# MAGIC propósito (ver `## Alcance recortado` de CLAUDE.md: "Silver, Gold ni
# MAGIC Quarantine para NovaDrive" no son responsabilidad de este proyecto). Los
# MAGIC nombres de tabla de abajo son directamente el valor de `entity`
# MAGIC (`customer`/`vehicle`/`sale`) sin traducir - no hay ninguna convención de
# MAGIC Silver con la que calzar, y este notebook no es el primer paso hacia
# MAGIC construir una: sigue siendo Bronze (append-only, sin canonicalizar, sin
# MAGIC `is_deleted`, sin MERGE) igual que el resto del alcance de este proyecto.
# MAGIC
# MAGIC **Relación con el Volume genérico `raw_events` (sin sufijo).** A
# MAGIC diferencia de Andes, que sigue teniendo un notebook separado
# MAGIC (`01_bronze_autoloader.py`) leyendo su Volume genérico de respaldo, **este
# MAGIC notebook es el PRIMERO y ÚNICO de Auto Loader para NovaDrive** - no existe
# MAGIC un equivalente que procese `raw_events` (el Volume de respaldo para
# MAGIC eventos con `entity` desconocido/payload corrupto, ver Fase 14/15). Ese
# MAGIC Volume queda sin materializar en tabla por ahora - asimetría real
# MAGIC respecto a Andes, documentada a propósito, no un descuido.
# MAGIC
# MAGIC **Correcciones aplicadas antes de considerarlo operable en producción**
# MAGIC (mismas 3 que se aplicaron en el notebook equivalente de Andes, 10 sep
# MAGIC 2026 - ver ahí para el detalle completo del razonamiento):
# MAGIC 1. Checkpoints/esquemas/registros de error viven en un Volume de estado
# MAGIC    separado (`bronze_pipeline_state`), nunca dentro de los Volumes fuente.
# MAGIC 2. `cloudFiles.rescuedDataColumn` (campos inesperados que sí parsean como
# MAGIC    JSON) + `badRecordsPath` (registros que ni siquiera son JSON válido) -
# MAGIC    dos redes de seguridad distintas para dos problemas distintos.
# MAGIC 3. `cloudFiles.maxFilesPerTrigger=1000` (valor conservador inicial).
# MAGIC
# MAGIC **IMPORTANTE para quien lo ejecute:** este notebook, al correr, SÍ
# MAGIC consume cómputo serverless real (lee y escribe datos). No se probó ni se
# MAGIC ejecutó en ningún momento al escribirlo - pedido explícito de no generar
# MAGIC ningún costo durante su desarrollo (mismo criterio que Andes). La primera
# MAGIC corrida real, y el monitoreo de crédito que genere, quedan a cargo de
# MAGIC quien lo ejecute. Ver el checklist de objetos que deben existir ANTES de
# MAGIC correrlo al final de este notebook - el Volume de estado (ítem 6) todavía
# MAGIC NO existe, verificado con una llamada real a la API al escribir este
# MAGIC notebook (10 sep 2026), no asumido.

# COMMAND ----------
# MAGIC %md
# MAGIC ## `availableNow=True` NO es una consulta Spark infinita
# MAGIC Mismo workspace que Andes (serverless-only, compartido - ver CLAUDE.md
# MAGIC decisión #7). En cómputo serverless, Databricks SOLO soporta
# MAGIC `Trigger.AvailableNow()` y `Trigger.Once()` - `Trigger.ProcessingTime(...)`
# MAGIC y `Trigger.Continuous(...)` fallan con `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`.
# MAGIC
# MAGIC `availableNow=True` procesa los archivos que estén disponibles EN ESE
# MAGIC MOMENTO y termina - no se queda escuchando indefinidamente. **La
# MAGIC continuidad real se logra en una capa distinta, no en este código**: un
# MAGIC Job de Databricks (`continuous.pause_status=UNPAUSED` vía Jobs API,
# MAGIC `max_concurrent_runs=1`) apuntando a este notebook - mismo mecanismo que
# MAGIC Andes ya verificó con evidencia real en su propio proyecto (8 corridas
# MAGIC automáticas consecutivas en ~4.7 minutos, sin intervención). Con
# MAGIC `max_concurrent_runs=1` el Job nunca dispara un ciclo nuevo antes de que
# MAGIC el anterior haya terminado - por eso puede relanzar ciclos sin duplicar
# MAGIC datos (ver también el checkpoint por entidad, más abajo). **Este Job NO
# MAGIC se creó** - es parte de lo que queda pendiente para quien ejecute esto.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Supuestos sobre los archivos de entrada (si esto deja de cumplirse, revisar todo lo demás)
# MAGIC Este pipeline asume que:
# MAGIC - Cada archivo `.jsonl` que sube el bridge es JSON completo e
# MAGIC   inmutable - una vez escrito, nunca se vuelve a modificar.
# MAGIC - Cada nombre de archivo es único (el bridge los nombra con
# MAGIC   `novadrive_batch_<fecha>_<hora>_<microsegundos>.jsonl`, generado fresco
# MAGIC   por cada archivo - ver `bridge/bridge_redpanda_to_databricks.py::upload_batch()`),
# MAGIC   así que Auto Loader nunca ve dos archivos con el mismo nombre y
# MAGIC   contenido distinto.
# MAGIC - El bridge nunca reescribe un archivo ya subido - solo sube archivos
# MAGIC   nuevos vía `PUT` a la Files API (`bridge/databricks_files.py`).
# MAGIC
# MAGIC **Confirmado real contra el código actual del bridge (10 sep 2026), no
# MAGIC asumido**: las 3 garantías de arriba se cumplen tal como está escrito hoy
# MAGIC - `upload_batch()` genera un nombre de archivo nuevo por cada grupo/subida
# MAGIC (con microsegundos, sin reutilizar nombres) y solo hace `PUT`, nunca un
# MAGIC `DELETE` seguido de un nuevo `PUT` al mismo nombre. Si esto cambia en el
# MAGIC bridge en el futuro, Auto Loader NO va a releer un archivo modificado -
# MAGIC ya lo marcó como procesado por nombre, no por contenido.

# COMMAND ----------
CATALOG = "novadrive_catalog"
BRONZE_SCHEMA = "bronze"

# Volumes FUENTE (Auto Loader SI los lee) - solo eventos de negocio reales,
# nada de metadata tecnica del pipeline mezclada ahi adentro.
RAW_BASE = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}"

# Volume de ESTADO (Auto Loader NUNCA lo lee como fuente) - guarda
# checkpoints, ubicaciones de esquema y registros de error de los 3
# procesos. Separado a proposito de los Volumes fuente - mismo
# razonamiento que Andes: si los checkpoints/esquemas vivieran adentro de
# raw_events_customer, Auto Loader corre el riesgo real de terminar
# tratando esos archivos tecnicos como si fueran eventos de negocio, o de
# que un reprocesamiento manual del Volume fuente arrastre sin querer el
# estado del pipeline junto con los datos.
STATE_VOLUME = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/bronze_pipeline_state"

spark.sql(f"USE CATALOG {CATALOG}")

# Un solo lugar con la config de los 3 procesos - (nombre para los logs y
# para las subcarpetas de estado, carpeta del Volume fuente, nombre de la
# tabla Delta de destino). Agregar una 4ta entidad el dia de manana es
# agregar una linea aca, no copiar/pegar un bloque de codigo entero.
# Tabla = nombre de entity directo (sin Silver con que calzar, ver arriba).
ENTIDADES = [
    ("customer", "raw_events_customer", "customer"),
    ("vehicle",  "raw_events_vehicle",  "vehicle"),
    ("sale",     "raw_events_sale",     "sale"),
]

# COMMAND ----------
# MAGIC %md
# MAGIC ## La función de ETL (una sola definición, se reusa 3 veces)
# MAGIC "3 procesos ETL dentro de un mismo pipeline" no significa pegar el mismo
# MAGIC bloque de código 3 veces con los nombres cambiados - significa 3
# MAGIC EJECUCIONES independientes de la misma lógica, una por origen - por eso
# MAGIC es una sola función, invocada 3 veces con distintos parámetros.

def procesar_bronze(nombre_entidad: str, carpeta_volume: str, tabla_destino: str) -> dict:
    """ETL de Bronze para UNA entidad (customer | vehicle | sale).

    1. Lee con Auto Loader SOLO los archivos nuevos del Volume FUENTE
       dedicado a esta entidad - nunca mezcla con los otros 2 orígenes.
    2. Los agrega tal cual llegan (sin transformar el payload todavía -
       eso sería trabajo de Silver, que está fuera de alcance para
       NovaDrive - ver `## Alcance recortado` de CLAUDE.md) a su propia
       tabla Delta.
    3. Usa checkpoint, ubicación de esquema y bad_records PROPIOS de esta
       entidad, todos dentro del Volume de ESTADO (nunca dentro del
       Volume fuente que esta leyendo) - si las 3 entidades compartieran
       un mismo checkpoint, Auto Loader se confundiria sobre que archivo
       de cual origen ya proceso; si el checkpoint viviera en el Volume
       fuente, se arriesgaria a mezclarse con los datos de negocio.
    4. Dos redes de seguridad distintas para dos problemas distintos:
       - `cloudFiles.rescuedDataColumn`: un registro que SI es JSON valido
         pero trae campos que no calzan con el esquema inferido no se
         descarta - los campos inesperados quedan visibles en la columna
         `_rescued_data`, la fila se carga igual.
       - `badRecordsPath`: un registro que ni siquiera es JSON valido
         (corrupto de verdad) no rompe el micro-batch entero - queda
         registrado en `bad_records/<entidad>` para revisar despues. (Nota
         de honestidad, misma que Andes: esta combinacion con Auto
         Loader/cloudFiles no se probo en una corrida real en este
         proyecto todavia - confirmar en la primera ejecucion real que
         efectivamente separa los casos como se espera.)
    5. Si la tabla `{CATALOG}.bronze.<tabla_destino>` todavía no existe,
       `.toTable()` la crea sola, con el esquema que Auto Loader infiera
       del primer archivo real que encuentre.

    Devuelve un dict con el resultado (para el resumen final de la celda
    de abajo) - nunca lanza la excepción hacia afuera, para que un
    problema en un origen no le impida correr a los otros 2 (el Job se
    marca como fallido recien al final, no se corta en el primer error).
    """
    source_path = f"{RAW_BASE}/{carpeta_volume}"
    checkpoint = f"{STATE_VOLUME}/checkpoints/{nombre_entidad}"
    schema_loc = f"{STATE_VOLUME}/schemas/{nombre_entidad}"
    bad_records = f"{STATE_VOLUME}/bad_records/{nombre_entidad}"
    tabla = f"{CATALOG}.{BRONZE_SCHEMA}.{tabla_destino}"

    print(f"[{nombre_entidad}] leyendo (fuente)  -> {source_path}")
    print(f"[{nombre_entidad}] checkpoint (estado) -> {checkpoint}")
    print(f"[{nombre_entidad}] escribiendo         -> {tabla}")

    try:
        stream = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_loc)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("badRecordsPath", bad_records)
            .option("cloudFiles.maxFilesPerTrigger", 1000)
            .load(source_path)
        )

        query = (
            stream.writeStream
            .format("delta")
            .outputMode("append")
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint)
            .option("mergeSchema", "true")
            .toTable(tabla)
        )

        query.awaitTermination()
        print(f"[{nombre_entidad}] OK - ciclo availableNow completado.")
        return {"entidad": nombre_entidad, "tabla": tabla, "estado": "OK", "error": None}

    except Exception as e:
        # No relanzamos aca: un origen con problemas no debe impedir que
        # los otros 2 procesos de este mismo pipeline corran igual. El
        # Job se marca como fallido recien al final (ver la celda de
        # resumen), una vez que los 3 ya tuvieron su oportunidad.
        print(f"[{nombre_entidad}] ERROR - {type(e).__name__}: {e}")
        return {"entidad": nombre_entidad, "tabla": tabla, "estado": "ERROR", "error": str(e)}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Los 3 procesos ETL, uno por origen - se corren en esta celda
# MAGIC Van secuenciales (customer, luego vehicle, luego sale) dentro del mismo
# MAGIC pipeline/notebook. Al ser 3 llamadas independientes (no un único
# MAGIC stream compartido), un fallo en una no bloquea a las otras dos.

resultados = [procesar_bronze(*fila) for fila in ENTIDADES]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Resumen de la corrida - qué salió bien y qué no
# MAGIC Revisar esto ANTES de asumir que la corrida completa salió bien - un
# MAGIC `ERROR` acá en un origen puntual no frena la celda entera (ver el
# MAGIC try/except de `procesar_bronze`), así que hay que mirarlo explícito.
# MAGIC Si hubo al menos un error, esta celda lanza una excepción a propósito -
# MAGIC así el Job que invoque este notebook queda marcado como **Failed**
# MAGIC (alerta real), aunque las entidades que sí salieron OK ya hayan quedado
# MAGIC escritas (no se revierten).

for r in resultados:
    marca = "OK" if r["estado"] == "OK" else "FALLO"
    print(f"[{marca}] {r['entidad']:10s} -> {r['tabla']}" + (f"  ({r['error']})" if r["error"] else ""))

hubo_errores = any(r["estado"] == "ERROR" for r in resultados)
if hubo_errores:
    raise RuntimeError(
        "Al menos un origen fallo en este ciclo de Bronze - ver el detalle impreso arriba. "
        "Las tablas de los origenes que SI salieron OK ya quedaron escritas (no se revierten)."
    )
print("\n[OK] Los 3 procesos ETL de Bronze terminaron sin errores.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verificación rápida - conteo real por tabla
# MAGIC Solo para confirmar de un vistazo que cada tabla recibió algo (o cero,
# MAGIC si esa entidad todavía no tiene ningún evento real en su Volume) - no
# MAGIC reemplaza revisar `_rescued_data`/`bad_records` si hace falta investigar
# MAGIC algo puntual.

display(spark.sql(f"""
    SELECT 'customer' AS tabla, COUNT(*) AS total_filas FROM {CATALOG}.bronze.customer
    UNION ALL
    SELECT 'vehicle',  COUNT(*) FROM {CATALOG}.bronze.vehicle
    UNION ALL
    SELECT 'sale',     COUNT(*) FROM {CATALOG}.bronze.sale
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checklist de objetos que deben existir ANTES de correr este notebook
# MAGIC
# MAGIC Verificado con llamadas reales a la Unity Catalog REST API (10 sep 2026,
# MAGIC solo lectura - nada de lo de abajo se creó al escribir este notebook):
# MAGIC
# MAGIC | # | Objeto | ¿Ya existe? | Notas |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Catálogo `novadrive_catalog` | Sí (confirmado real) | |
# MAGIC | 2 | Schema `novadrive_catalog.bronze` | Sí (confirmado real) | |
# MAGIC | 3 | Volume `novadrive_catalog.bronze.raw_events_customer` | Sí (confirmado real) | Fuente de este pipeline. |
# MAGIC | 4 | Volume `novadrive_catalog.bronze.raw_events_vehicle` | Sí (confirmado real) | Fuente de este pipeline. |
# MAGIC | 5 | Volume `novadrive_catalog.bronze.raw_events_sale` | Sí (confirmado real) | Fuente de este pipeline. |
# MAGIC | 6 | Volume `novadrive_catalog.bronze.bronze_pipeline_state` | **NO existe todavía** (`404` real confirmado) | Hay que crearlo antes de la primera corrida - ver nota abajo. |
# MAGIC | 7 | Permisos del principal que ejecuta el notebook | Parcialmente verificado | El token usado para escribir este notebook (`josuequee@gmail.com`) ya creó `bronze` y los 3 Volumes fuente en este mismo catálogo en una sesión anterior (evidencia real de `CREATE SCHEMA`/`CREATE VOLUME`) - pero quien EJECUTE el notebook puede ser un principal distinto (cluster/Job service principal); falta verificar ese principal específico tiene `USE CATALOG`/`USE SCHEMA` sobre `bronze`, `READ VOLUME` sobre los 3 Volumes fuente, `READ VOLUME`+`WRITE VOLUME` sobre `bronze_pipeline_state`, y `CREATE TABLE` sobre el schema. |
# MAGIC | 8 | Tablas `bronze.customer`/`bronze.vehicle`/`bronze.sale` | No hace falta crearlas a mano | `.toTable()` las crea solas en el primer `availableNow` exitoso, con el esquema que Auto Loader infiera. |
# MAGIC
# MAGIC **El Volume `bronze_pipeline_state` (ítem 6) todavía no se creó a
# MAGIC propósito.** Se puede crear igual que los otros 3 - vía la Unity Catalog
# MAGIC REST API (`POST /api/2.0/unity-catalog/volumes`, `volume_type: MANAGED`),
# MAGIC sin costo de cómputo (confirmado en la práctica, Fase 14/15 de
# MAGIC CLAUDE.md). No se creó automáticamente al escribir este notebook - es
# MAGIC parte de lo que hay que decidir/ejecutar antes de la primera corrida
# MAGIC real, mismo criterio que Andes.

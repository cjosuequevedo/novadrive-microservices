# Databricks notebook source
# MAGIC %md
# MAGIC # 02b - Silver: 3 procesos ETL (uno por tabla Bronze), homologación +
# MAGIC dedup + MERGE + stubs + Quarantine. `trigger(availableNow=True)`.
# MAGIC
# MAGIC **Qué hace este notebook, en una frase:** lee cada una de las 3 tablas
# MAGIC Bronze de NovaDrive (`bronze.customer`/`vehicle`/`sale`, pobladas por
# MAGIC `01b_bronze_autoloader_por_tipo.py` - ya corrió al menos una vez, ver el
# MAGIC checklist al final), homologa cada registro al esquema canónico, y lo
# MAGIC mergea en su tabla Silver correspondiente (`silver.customer`/`vehicle`/`sale`).
# MAGIC
# MAGIC **Cambio de alcance explícito (10 sep 2026).** Silver/Quarantine estaban
# MAGIC fuera del alcance original de NovaDrive (ver CLAUDE.md, `## Alcance
# MAGIC recortado`, acordado el 9 sep 2026 por quien encargó el proyecto y el
# MAGIC líder de equipo, justo para no repetir el desvío de tiempo real que ya
# MAGIC pasó en Andes). CJ confirmó explícitamente que esto ahora es necesario
# MAGIC para el trabajo del compañero encargado de la arquitectura medallón, que
# MAGIC va a ejecutar este notebook y su DDL manualmente - mismo criterio que ya
# MAGIC se usó con Bronze (entregable, no ejecutado por quien lo escribe).
# MAGIC
# MAGIC **Mismo patrón exacto que `02b_silver_merge_por_tipo.py` de Andes** (leído
# MAGIC completo antes de escribir este, para replicar estilo y lógica, no solo
# MAGIC la idea general) - la única parte reescrita entidad por entidad es la
# MAGIC homologación (`homologar_customer/vehicle/sale`, más abajo), con los
# MAGIC nombres de campo reales de NovaDrive (`ND_CUSTOMER`/`ND_INVENTORY_UNIT`/
# MAGIC `ND_SALES_ORDER`, confirmados contra filas reales de Bronze pasadas por
# MAGIC CJ, no inventados). La orquestación (dedup, MERGE, stubs, Quarantine,
# MAGIC estado separado, tolerancia a fallo parcial) es la misma arquitectura,
# MAGIC sin reinventar nada.
# MAGIC
# MAGIC **A diferencia de Andes, NovaDrive no tiene un notebook Silver viejo que
# MAGIC cubra el Volume/tabla genérica de respaldo** (`raw_events` sin sufijo) -
# MAGIC mismo hueco ya documentado en `01b_bronze_autoloader_por_tipo.py`: ese
# MAGIC Volume no tiene ni siquiera una tabla Bronze todavía, así que tampoco
# MAGIC puede tener Silver. Asimetría real frente a Andes, no un descuido.
# MAGIC
# MAGIC **IMPORTANTE para quien lo ejecute:** este notebook, al correr, SÍ
# MAGIC consume cómputo serverless real. No se probó ni se ejecutó en ningún
# MAGIC momento al escribirlo - pedido explícito de no generar ningún costo
# MAGIC durante su desarrollo (mismo criterio que Andes y que el propio Bronze de
# MAGIC NovaDrive). La primera corrida real, y el monitoreo de crédito que
# MAGIC genere, quedan a cargo de quien lo ejecute. **Ver el checklist de objetos
# MAGIC que deben existir ANTES de correrlo al final de este notebook - a
# MAGIC diferencia de Bronze, acá varias tablas NO se crean solas y hay que
# MAGIC aplicarles su DDL primero (`databricks/sql/ddl_silver.sql`, nuevo,
# MAGIC tampoco aplicado todavía).**

# COMMAND ----------
# MAGIC %md
# MAGIC ## `availableNow=True` no es una consulta infinita (mismo motivo que en 01b)
# MAGIC Mismo workspace serverless-only, compartido con Andes (ver CLAUDE.md
# MAGIC decisión #7) - solo soporta `Trigger.AvailableNow()`/`Trigger.Once()`,
# MAGIC nunca `Trigger.ProcessingTime(...)`/`Trigger.Continuous(...)`. Este
# MAGIC notebook procesa lo disponible y termina. La continuidad real se logra
# MAGIC con un Job (Lakeflow Jobs en la UI actual) en trigger `continuous` +
# MAGIC `max_concurrent_runs=1` que invoque este notebook - mismo mecanismo que
# MAGIC Andes ya verificó con evidencia real en su propio proyecto (8 corridas
# MAGIC automáticas consecutivas en ~4.7 minutos, sin intervención manual).
# MAGIC **Ese Job NO se creó** - queda pendiente para quien ejecute esto.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Supuestos de los que depende este pipeline (si dejan de cumplirse, revisar todo lo demás)
# MAGIC - Las tablas Bronze (`bronze.customer`/`vehicle`/`sale`) son
# MAGIC   **append-only** - nunca se actualiza ni se borra una fila ya escrita ahí
# MAGIC   (esa es la garantía real de Auto Loader en `01b`).
# MAGIC - El campo `payload` es el envelope JSON completo tal cual lo armó la app
# MAGIC   (`event_id`/`entity`/`operation`/`occurred_at`/`before`/`after`,
# MAGIC   confirmado real contra filas reales de Bronze) - nunca se transforma
# MAGIC   antes de Bronze.
# MAGIC - `event_id` es único por evento real - el dedup de este notebook
# MAGIC   (`row_number()` ordenando por `occurred_at desc, event_id desc`) confía
# MAGIC   en eso para quedarse con la versión más nueva de cada clave dentro de
# MAGIC   un mismo micro-batch.
# MAGIC - **`occurred_at` NO es 100% confiable como único criterio de orden en
# MAGIC   los MERGE de customer/vehicle** - un stub (creado desde una `sale` que
# MAGIC   referencia un `customer`/`vehicle` que todavía no llegó, ver el proceso
# MAGIC   3/3 más abajo) hereda el `occurred_at` de esa venta, que casi siempre es
# MAGIC   POSTERIOR a la creación real del customer/vehicle. Por eso la condición
# MAGIC   de `whenMatchedUpdate` de customer/vehicle es
# MAGIC   `pendiente_completar = true OR occurred_at >= t.occurred_at`, no
# MAGIC   `occurred_at >= t.occurred_at` a secas - **no revertir esto**, es un bug
# MAGIC   real ya encontrado y corregido en Andes (su hallazgo #10 de CLAUDE.md):
# MAGIC   sin el `OR`, un stub puede quedar así para siempre, sin ningún error
# MAGIC   visible.

# COMMAND ----------
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType,
    TimestampType, DecimalType, DateType,
)
from delta.tables import DeltaTable

CATALOG = "novadrive_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# Volume de ESTADO de Silver - separado del de Bronze (bronze_pipeline_state)
# a proposito: son pipelines distintos, con checkpoints distintos, y mezclar
# el estado de una capa con el de la otra es el mismo tipo de confusion que
# se corrigio en 01b al sacar los checkpoints de los Volumes fuente.
SILVER_STATE_VOLUME = f"/Volumes/{CATALOG}/{SILVER_SCHEMA}/silver_pipeline_state"

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------
# MAGIC %md ### Homologación canónica (la parte reescrita para NovaDrive - campos reales confirmados contra Bronze real, 10 sep 2026)

# COMMAND ----------
def _dec(v):
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except InvalidOperation:
        return None


def _validar_vin(vin_raw):
    # Mismo charset que ya valida la app (services/inventory/schemas.py:
    # _VIN_CHARSET, sin I/O/Q) - se revalida acá tambien, defensivamente,
    # no confiamos ciegamente en que el origen siempre lo cumplio.
    if not vin_raw:
        return None, False, "VIN vacio"
    v = re.sub(r"\s+", "", str(vin_raw)).upper()
    ok = bool(re.match(r"^[A-HJ-NPR-Z0-9]{17}$", v))
    return v, ok, (None if ok else f"VIN invalido: {vin_raw!r}")


def _validar_anio(anio_raw):
    # Mismo rango que ya valida la app (services/inventory/schemas.py:
    # model_year Field(ge=1900, le=2100)) - no el "anio actual + 1" que
    # usa Andes, para quedar consistente con la validacion real de
    # NovaDrive, no con la de otro proyecto.
    try:
        a = int(anio_raw)
        if 1900 <= a <= 2100:
            return a, True, None
        return a, False, f"Model year fuera de rango [1900-2100]: {a}"
    except (TypeError, ValueError):
        return None, False, f"Model year invalido: {anio_raw!r}"


def homologar_customer(payload, sistema):
    record = payload.get("after") or payload.get("before") or {}
    source_record_id = record.get("CUSTOMER_CODE")
    doc = record.get("DOCUMENT_NUMBER")
    estado_map = {"A": "ACTIVE", "S": "SUSPENDED", "X": "CLOSED"}
    motivos = [] if doc else ["document_number vacio"]
    row = {
        "customer_key": f"{sistema}|{source_record_id}",
        "source_system": sistema,
        "source_record_id": source_record_id,
        "document_number": doc,
        "full_name": record.get("FULL_NAME"),
        "email_address": (record.get("EMAIL_ADDRESS") or "").strip().lower() or None,
        "mobile_phone": record.get("MOBILE_PHONE"),
        "mailing_address": record.get("MAILING_ADDRESS"),
        "birth_date": record.get("BIRTH_DATE"),
        "status": estado_map.get(record.get("CUSTOMER_STATUS"), record.get("CUSTOMER_STATUS")),
        "is_deleted": payload["operation"] == "d",
        "deleted_at": payload["occurred_at"] if payload["operation"] == "d" else None,
        "occurred_at": payload["occurred_at"],
        "event_id": payload["event_id"],
        "updated_at": payload["occurred_at"],
        "pendiente_completar": False,
    }
    return row, motivos


def homologar_vehicle(payload, sistema):
    record = payload.get("after") or payload.get("before") or {}
    source_record_id = record.get("CHASSIS_NUMBER")
    vin, vin_ok, motivo_vin = _validar_vin(record.get("CHASSIS_NUMBER"))
    anio, anio_ok, motivo_anio = _validar_anio(record.get("MODEL_YEAR"))
    motivos = [m for m in (motivo_vin, motivo_anio) if m]
    disp_map = {"AVL": "AVAILABLE", "HOLD": "HELD", "SOLD": "SOLD"}
    row = {
        "vehicle_key": f"{sistema}|{source_record_id}",
        "source_system": sistema,
        "source_record_id": source_record_id,
        "vin": vin,
        "branch_code": record.get("BRANCH_CODE"),
        "brand": record.get("BRAND_NAME"),
        "model": record.get("MODEL_NAME"),
        "model_year": anio,
        "exterior_colour": record.get("EXTERIOR_COLOUR"),
        "list_amount": _dec(record.get("LIST_AMOUNT")),
        "availability": disp_map.get(record.get("AVAILABILITY_CODE"), record.get("AVAILABILITY_CODE")),
        "is_deleted": payload["operation"] == "d",
        "deleted_at": payload["occurred_at"] if payload["operation"] == "d" else None,
        "occurred_at": payload["occurred_at"],
        "event_id": payload["event_id"],
        "updated_at": payload["occurred_at"],
        "pendiente_completar": False,
    }
    return row, motivos


def homologar_sale(payload, sistema):
    record = payload.get("after") or payload.get("before") or {}
    source_record_id = record.get("ORDER_NUMBER")
    buyer_code = record.get("BUYER_CODE")
    chassis_number = record.get("CHASSIS_NUMBER")
    row = {
        "sale_key": f"{sistema}|{source_record_id}",
        "source_system": sistema,
        "source_record_id": source_record_id,
        "customer_key": f"{sistema}|{buyer_code}" if buyer_code is not None else None,
        "vehicle_key": f"{sistema}|{chassis_number}" if chassis_number is not None else None,
        "sales_agent_code": record.get("SALES_AGENT_CODE"),
        "branch_code": record.get("FULFILLMENT_BRANCH"),
        "ordered_at": record.get("ORDERED_AT"),
        "gross_amount": _dec(record.get("GROSS_AMOUNT")),
        "discount_pct": _dec(record.get("DISCOUNT_PCT")),
        "tax_amount": _dec(record.get("TAX_AMOUNT")),
        "net_amount": _dec(record.get("NET_AMOUNT")),
        "currency_code": record.get("CURRENCY_CODE"),
        # OPEN/APPROVED/INVOICED/CANCELLED ya son canonicos en ingles - sin
        # mapeo, a diferencia de customer/vehicle (que si vienen en codigos
        # cortos que hay que traducir).
        "status": record.get("ORDER_STATUS"),
        "invoice_reference": record.get("INVOICE_REFERENCE"),
        "is_deleted": payload["operation"] == "d",
        "deleted_at": payload["occurred_at"] if payload["operation"] == "d" else None,
        "occurred_at": payload["occurred_at"],
        "event_id": payload["event_id"],
        "updated_at": payload["occurred_at"],
    }
    return row, []


# COMMAND ----------
# MAGIC %md ### Schemas explícitos (evitan ambigüedad al inferir el esquema de DataFrames armados en el driver)

# COMMAND ----------
SCHEMA_CUSTOMER = StructType([
    StructField("customer_key", StringType()), StructField("source_system", StringType()),
    StructField("source_record_id", StringType()), StructField("document_number", StringType()),
    StructField("full_name", StringType()), StructField("email_address", StringType()),
    StructField("mobile_phone", StringType()), StructField("mailing_address", StringType()),
    StructField("birth_date", StringType()), StructField("status", StringType()),
    StructField("is_deleted", BooleanType()), StructField("deleted_at", StringType()),
    StructField("occurred_at", StringType()), StructField("event_id", StringType()),
    StructField("updated_at", StringType()), StructField("pendiente_completar", BooleanType()),
])

SCHEMA_VEHICLE = StructType([
    StructField("vehicle_key", StringType()), StructField("source_system", StringType()),
    StructField("source_record_id", StringType()), StructField("vin", StringType()),
    StructField("branch_code", StringType()), StructField("brand", StringType()),
    StructField("model", StringType()), StructField("model_year", IntegerType()),
    StructField("exterior_colour", StringType()), StructField("list_amount", DecimalType(15, 2)),
    StructField("availability", StringType()), StructField("is_deleted", BooleanType()),
    StructField("deleted_at", StringType()), StructField("occurred_at", StringType()),
    StructField("event_id", StringType()), StructField("updated_at", StringType()),
    StructField("pendiente_completar", BooleanType()),
])

SCHEMA_SALE = StructType([
    StructField("sale_key", StringType()), StructField("source_system", StringType()),
    StructField("source_record_id", StringType()), StructField("customer_key", StringType()),
    StructField("vehicle_key", StringType()), StructField("sales_agent_code", StringType()),
    StructField("branch_code", StringType()), StructField("ordered_at", StringType()),
    StructField("gross_amount", DecimalType(16, 2)), StructField("discount_pct", DecimalType(7, 4)),
    StructField("tax_amount", DecimalType(16, 2)), StructField("net_amount", DecimalType(16, 2)),
    StructField("currency_code", StringType()), StructField("status", StringType()),
    StructField("invoice_reference", StringType()), StructField("is_deleted", BooleanType()),
    StructField("deleted_at", StringType()), StructField("occurred_at", StringType()),
    StructField("event_id", StringType()), StructField("updated_at", StringType()),
])


# COMMAND ----------
# MAGIC %md ### Quarantine compartida
# MAGIC Un solo inbox de triage para los 3 procesos - no hace falta una tabla de
# MAGIC cuarentena por entidad, el `entity`/`reason` de cada fila ya identifica
# MAGIC de cuál vino.

# COMMAND ----------
def _encolar_cuarentena(cuarentena):
    if not cuarentena:
        return
    ahora = datetime.now(timezone.utc)
    filas_q = [
        {"event_id": e, "entity": en, "source_table": st, "operation": op, "payload": p, "reason": m, "created_at": ahora}
        for (e, en, st, op, p, m) in cuarentena
    ]
    spark.createDataFrame(filas_q).write.format("delta").mode("append").saveAsTable(f"{CATALOG}.quarantine.events")


# COMMAND ----------
# MAGIC %md ### Proceso ETL 1/3 - Bronze.customer -> Silver.customer

# COMMAND ----------
def _batch_customer(df_microbatch, batch_id):
    if df_microbatch.isEmpty():
        return
    filas = df_microbatch.select("payload").collect()
    customers, cuarentena = [], []

    for r in filas:
        payload_raw = r["payload"]
        try:
            payload = json.loads(payload_raw)
        except Exception as e:
            cuarentena.append((None, "customer", None, None, payload_raw, f"JSON invalido: {e}"))
            continue

        entity = payload.get("entity")
        sistema = payload.get("source_system", "NOVADRIVE")
        event_id = payload.get("event_id")
        source_table = payload.get("source_table")
        operation = payload.get("operation")

        # Defensivo: bronze.customer deberia tener SOLO entity="customer"
        # (asi lo garantiza 01b), pero no confiamos ciegamente en el origen -
        # si algo raro llega, a cuarentena, nunca se descarta en silencio.
        if entity != "customer":
            cuarentena.append((event_id, entity, source_table, operation, payload_raw,
                                f"entity inesperada en bronze.customer: {entity!r}"))
            continue

        try:
            row, motivos = homologar_customer(payload, sistema)
            if motivos:
                cuarentena.append((event_id, entity, source_table, operation, payload_raw, "; ".join(motivos)))
            else:
                customers.append(row)
        except Exception as e:
            cuarentena.append((event_id, entity, source_table, operation, payload_raw, f"error homologando: {e}"))

    _encolar_cuarentena(cuarentena)

    if not customers:
        return

    df = (
        spark.createDataFrame(customers, schema=SCHEMA_CUSTOMER)
        .withColumn("birth_date", col("birth_date").cast(DateType()))
        .withColumn("deleted_at", col("deleted_at").cast(TimestampType()))
        .withColumn("occurred_at", col("occurred_at").cast(TimestampType()))
        .withColumn("updated_at", col("updated_at").cast(TimestampType()))
    )
    w = Window.partitionBy("customer_key").orderBy(col("occurred_at").desc(), col("event_id").desc())
    df = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

    DeltaTable.forName(spark, f"{CATALOG}.silver.customer").alias("t").merge(
        df.alias("s"), "t.customer_key = s.customer_key"
    ).whenMatchedUpdate(
        # Ver la seccion de "Supuestos" arriba - no quitar el
        # "pendiente_completar = true OR": hallazgo #10 de CLAUDE.md de Andes.
        condition="t.pendiente_completar = true OR s.occurred_at >= t.occurred_at",
        set={c: f"s.{c}" for c in SCHEMA_CUSTOMER.fieldNames() if c != "customer_key"} | {"pendiente_completar": "false"},
    ).whenNotMatchedInsertAll().execute()


# COMMAND ----------
# MAGIC %md ### Proceso ETL 2/3 - Bronze.vehicle -> Silver.vehicle

# COMMAND ----------
def _batch_vehicle(df_microbatch, batch_id):
    if df_microbatch.isEmpty():
        return
    filas = df_microbatch.select("payload").collect()
    vehicles, cuarentena = [], []

    for r in filas:
        payload_raw = r["payload"]
        try:
            payload = json.loads(payload_raw)
        except Exception as e:
            cuarentena.append((None, "vehicle", None, None, payload_raw, f"JSON invalido: {e}"))
            continue

        entity = payload.get("entity")
        sistema = payload.get("source_system", "NOVADRIVE")
        event_id = payload.get("event_id")
        source_table = payload.get("source_table")
        operation = payload.get("operation")

        if entity != "vehicle":
            cuarentena.append((event_id, entity, source_table, operation, payload_raw,
                                f"entity inesperada en bronze.vehicle: {entity!r}"))
            continue

        try:
            row, motivos = homologar_vehicle(payload, sistema)
            if motivos:
                cuarentena.append((event_id, entity, source_table, operation, payload_raw, "; ".join(motivos)))
            else:
                vehicles.append(row)
        except Exception as e:
            cuarentena.append((event_id, entity, source_table, operation, payload_raw, f"error homologando: {e}"))

    _encolar_cuarentena(cuarentena)

    if not vehicles:
        return

    df = (
        spark.createDataFrame(vehicles, schema=SCHEMA_VEHICLE)
        .withColumn("deleted_at", col("deleted_at").cast(TimestampType()))
        .withColumn("occurred_at", col("occurred_at").cast(TimestampType()))
        .withColumn("updated_at", col("updated_at").cast(TimestampType()))
    )
    w = Window.partitionBy("vehicle_key").orderBy(col("occurred_at").desc(), col("event_id").desc())
    df = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

    DeltaTable.forName(spark, f"{CATALOG}.silver.vehicle").alias("t").merge(
        df.alias("s"), "t.vehicle_key = s.vehicle_key"
    ).whenMatchedUpdate(
        condition="t.pendiente_completar = true OR s.occurred_at >= t.occurred_at",
        set={c: f"s.{c}" for c in SCHEMA_VEHICLE.fieldNames() if c != "vehicle_key"} | {"pendiente_completar": "false"},
    ).whenNotMatchedInsertAll().execute()


# COMMAND ----------
# MAGIC %md ### Proceso ETL 3/3 - Bronze.sale -> Silver.sale (+ stubs diferidos)
# MAGIC Este es el único de los 3 que también puede escribir en las OTRAS 2
# MAGIC tablas Silver (stubs de `customer`/`vehicle` referenciados que todavía no
# MAGIC llegaron - una `sale` de NovaDrive referencia AMBOS, `BUYER_CODE` Y
# MAGIC `CHASSIS_NUMBER`, así que puede necesitar hasta 2 stubs por venta, no
# MAGIC solo 1). El orden en que corran los 3 procesos de este notebook NO
# MAGIC afecta la corrección final: si el stub se crea acá y el proceso 1/3 o
# MAGIC 2/3 ya corrió antes en este mismo ciclo, no pasa nada - lo completará en
# MAGIC el próximo ciclo, gracias al `pendiente_completar = true OR ...` de
# MAGIC arriba.

# COMMAND ----------
def _batch_sale(df_microbatch, batch_id):
    if df_microbatch.isEmpty():
        return
    filas = df_microbatch.select("payload").collect()
    sales, cuarentena = [], []

    for r in filas:
        payload_raw = r["payload"]
        try:
            payload = json.loads(payload_raw)
        except Exception as e:
            cuarentena.append((None, "sale", None, None, payload_raw, f"JSON invalido: {e}"))
            continue

        entity = payload.get("entity")
        sistema = payload.get("source_system", "NOVADRIVE")
        event_id = payload.get("event_id")
        source_table = payload.get("source_table")
        operation = payload.get("operation")

        if entity != "sale":
            cuarentena.append((event_id, entity, source_table, operation, payload_raw,
                                f"entity inesperada en bronze.sale: {entity!r}"))
            continue

        try:
            row, _ = homologar_sale(payload, sistema)
            sales.append(row)
        except Exception as e:
            cuarentena.append((event_id, entity, source_table, operation, payload_raw, f"error homologando: {e}"))

    _encolar_cuarentena(cuarentena)

    if not sales:
        return

    keys_cust_existentes = set(r[0] for r in spark.table(f"{CATALOG}.silver.customer").select("customer_key").collect())
    keys_veh_existentes = set(r[0] for r in spark.table(f"{CATALOG}.silver.vehicle").select("vehicle_key").collect())

    stubs_cust, stubs_veh = [], []
    for s in sales:
        ck = s["customer_key"]
        if ck and ck not in keys_cust_existentes:
            stubs_cust.append({
                "customer_key": ck, "source_system": s["source_system"],
                "source_record_id": ck.split("|", 1)[-1], "document_number": None,
                "full_name": "CUSTOMER PENDING SYNC", "email_address": None, "mobile_phone": None,
                "mailing_address": None, "birth_date": None, "status": "ACTIVE",
                "is_deleted": False, "deleted_at": None, "occurred_at": s["occurred_at"],
                "event_id": s["event_id"], "updated_at": s["occurred_at"], "pendiente_completar": True,
            })
            keys_cust_existentes.add(ck)
        vk = s["vehicle_key"]
        if vk and vk not in keys_veh_existentes:
            stubs_veh.append({
                "vehicle_key": vk, "source_system": s["source_system"],
                "source_record_id": vk.split("|", 1)[-1], "vin": None, "branch_code": None,
                "brand": "UNKNOWN", "model": "VEHICLE PENDING SYNC", "model_year": None,
                "exterior_colour": None, "list_amount": None, "availability": "SOLD",
                "is_deleted": False, "deleted_at": None, "occurred_at": s["occurred_at"],
                "event_id": s["event_id"], "updated_at": s["occurred_at"], "pendiente_completar": True,
            })
            keys_veh_existentes.add(vk)

    if stubs_cust:
        df_s = (
            spark.createDataFrame(stubs_cust, schema=SCHEMA_CUSTOMER)
            .withColumn("birth_date", col("birth_date").cast(DateType()))
            .withColumn("occurred_at", col("occurred_at").cast(TimestampType()))
            .withColumn("updated_at", col("updated_at").cast(TimestampType()))
        )
        DeltaTable.forName(spark, f"{CATALOG}.silver.customer").alias("t").merge(
            df_s.alias("s"), "t.customer_key = s.customer_key"
        ).whenNotMatchedInsertAll().execute()

    if stubs_veh:
        df_s = (
            spark.createDataFrame(stubs_veh, schema=SCHEMA_VEHICLE)
            .withColumn("occurred_at", col("occurred_at").cast(TimestampType()))
            .withColumn("updated_at", col("updated_at").cast(TimestampType()))
        )
        DeltaTable.forName(spark, f"{CATALOG}.silver.vehicle").alias("t").merge(
            df_s.alias("s"), "t.vehicle_key = s.vehicle_key"
        ).whenNotMatchedInsertAll().execute()

    df = (
        spark.createDataFrame(sales, schema=SCHEMA_SALE)
        .withColumn("ordered_at", col("ordered_at").cast(TimestampType()))
        .withColumn("deleted_at", col("deleted_at").cast(TimestampType()))
        .withColumn("occurred_at", col("occurred_at").cast(TimestampType()))
        .withColumn("updated_at", col("updated_at").cast(TimestampType()))
    )
    w = Window.partitionBy("sale_key").orderBy(col("occurred_at").desc(), col("event_id").desc())
    df = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

    DeltaTable.forName(spark, f"{CATALOG}.silver.sale").alias("t").merge(
        df.alias("s"), "t.sale_key = s.sale_key"
    ).whenMatchedUpdate(
        # A diferencia de customer/vehicle: nada crea un stub de "sale", asi
        # que no hace falta el "pendiente_completar = true OR" aca.
        condition="s.occurred_at >= t.occurred_at",
        set={c: f"s.{c}" for c in SCHEMA_SALE.fieldNames() if c != "sale_key"},
    ).whenNotMatchedInsertAll().execute()


# COMMAND ----------
# MAGIC %md ### Runner genérico - dispara un proceso Bronze->Silver y espera a que termine
# MAGIC Igual que en `01b`: una sola función reusada 3 veces, no 3 bloques de
# MAGIC código idénticos con el nombre cambiado. El try/except está ACÁ (no
# MAGIC dentro de cada `_batch_*`) para que un error real de Spark al armar o
# MAGIC lanzar el stream (no solo un error de homologación de una fila - esos ya
# MAGIC van a Quarantine) tampoco tumbe a los otros 2 procesos.

def procesar_silver(nombre_entidad: str, tabla_bronze: str, funcion_batch) -> dict:
    checkpoint = f"{SILVER_STATE_VOLUME}/checkpoints/{nombre_entidad}"
    tabla_bronze_completa = f"{CATALOG}.{BRONZE_SCHEMA}.{tabla_bronze}"

    print(f"[{nombre_entidad}] leyendo bronze  -> {tabla_bronze_completa}")
    print(f"[{nombre_entidad}] checkpoint      -> {checkpoint}")

    try:
        query = (
            spark.readStream
            .format("delta")
            .option("maxFilesPerTrigger", 1000)
            .table(tabla_bronze_completa)
            .writeStream
            .foreachBatch(funcion_batch)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        print(f"[{nombre_entidad}] OK - ciclo availableNow completado.")
        return {"entidad": nombre_entidad, "estado": "OK", "error": None}

    except Exception as e:
        print(f"[{nombre_entidad}] ERROR - {type(e).__name__}: {e}")
        return {"entidad": nombre_entidad, "estado": "ERROR", "error": str(e)}


# COMMAND ----------
# MAGIC %md ## Los 3 procesos ETL, uno por origen - se corren en esta celda
# MAGIC Secuenciales (customer, luego vehicle, luego sale). El orden es
# MAGIC arbitrario para la corrección (ver nota en la sección de sale) - se
# MAGIC mantiene el mismo orden que en `01b` solo por legibilidad de los logs.

resultados = [
    procesar_silver("customer", "customer", _batch_customer),
    procesar_silver("vehicle", "vehicle", _batch_vehicle),
    procesar_silver("sale", "sale", _batch_sale),
]

# COMMAND ----------
# MAGIC %md ## Resumen de la corrida - qué salió bien y qué no
# MAGIC Mismo criterio que en `01b`: un error puntual no frena a los otros 2
# MAGIC procesos, pero si hubo al menos uno, esta celda lanza una excepción a
# MAGIC propósito para que el Job quede marcado como **Failed** (alerta real).

for r in resultados:
    marca = "OK" if r["estado"] == "OK" else "FALLO"
    print(f"[{marca}] {r['entidad']:10s}" + (f"  ({r['error']})" if r["error"] else ""))

hubo_errores = any(r["estado"] == "ERROR" for r in resultados)
if hubo_errores:
    raise RuntimeError(
        "Al menos un origen fallo en este ciclo de Silver - ver el detalle impreso arriba. "
        "Las entidades que SI salieron OK ya quedaron mergeadas (no se revierten)."
    )
print("\n[OK] Los 3 procesos ETL de Silver terminaron sin errores.")

# COMMAND ----------
# MAGIC %md ## Verificación rápida - conteo real por tabla Silver

# COMMAND ----------
display(spark.sql(f"""
    SELECT 'customer' AS tabla, COUNT(*) AS total_filas FROM {CATALOG}.silver.customer
    UNION ALL
    SELECT 'vehicle',  COUNT(*) FROM {CATALOG}.silver.vehicle
    UNION ALL
    SELECT 'sale',     COUNT(*) FROM {CATALOG}.silver.sale
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checklist de objetos que deben existir ANTES de correr este notebook
# MAGIC
# MAGIC Verificado con llamadas reales de solo lectura a la Unity Catalog REST
# MAGIC API (10 sep 2026) - nada de lo de abajo se creó al escribir este notebook:
# MAGIC
# MAGIC | # | Objeto | ¿Ya existe? | Notas |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Tablas `bronze.customer`/`vehicle`/`sale` | **Sí (confirmado real)** | `01b` ya corrió al menos una vez - las 3 tablas existen con datos reales. |
# MAGIC | 2 | Schema `novadrive_catalog.silver` | **NO existe todavía** (`404` real) | |
# MAGIC | 3 | Schema `novadrive_catalog.quarantine` | **NO existe todavía** (`404` real) | |
# MAGIC | 4 | Tablas `silver.customer`/`silver.vehicle`/`silver.sale` | **NO existen todavía** | **A diferencia de Bronze, acá el `MERGE` de `DeltaTable.forName(...)` exige que la tabla YA exista - `.toTable()` no aplica.** Aplicar `databricks/sql/ddl_silver.sql` (nuevo) antes de la primera corrida. |
# MAGIC | 5 | Tabla `quarantine.events` | **NO existe todavía** | Mismo DDL, sección de `quarantine.events`. |
# MAGIC | 6 | Volume `novadrive_catalog.silver.silver_pipeline_state` | **NO existe todavía** (`404` real) | Nuevo, para los checkpoints de este notebook - separado del de Bronze a propósito. Se crea igual que los demás Volumes (Unity Catalog REST API, `POST /api/2.0/unity-catalog/volumes`, sin costo de cómputo). |
# MAGIC | 7 | Permisos del principal que ejecuta el notebook | Verificar | `SELECT` sobre las 3 tablas Bronze, `MODIFY`/`SELECT` sobre las 3 tablas Silver y sobre `quarantine.events`, `READ VOLUME`+`WRITE VOLUME` sobre `silver_pipeline_state`. |
# MAGIC
# MAGIC **Ninguno de los ítems 2-6 se creó automáticamente al escribir este
# MAGIC notebook** - es parte de lo que hay que decidir/ejecutar antes de la
# MAGIC primera corrida real, igual que se dejó pendiente `bronze_pipeline_state`
# MAGIC en `01b`. Todo esto vive DENTRO de `novadrive_catalog` únicamente -
# MAGIC `andes_catalog` no se leyó, escribió ni listó en ningún momento al
# MAGIC escribir este notebook.

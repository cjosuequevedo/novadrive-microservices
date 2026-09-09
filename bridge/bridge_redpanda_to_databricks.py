"""
bridge_redpanda_to_databricks.py - Puente Redpanda (propio de NovaDrive)
-> `novadrive_catalog.bronze.events` (Databricks). Consume los 3
topicos de NovaDrive, enriquece con metadatos Kafka reales
(topic/partition/offset/timestamp) y hace INSERT directo, en lote, via
la Statement Execution API - SIN pasar por un Volume + Auto Loader
como hace Andes.

Decision de diseno (marcada, no asumida en silencio - ver CLAUDE.md):
Andes sube JSONL a un Volume y usa Auto Loader (`01_bronze_autoloader.py`,
`availableNow`) porque su Silver depende de las garantias de
checkpointing/exactly-once de Auto Loader sobre archivos. NovaDrive NO
tiene Silver (fuera de alcance, ver `## Alcance recortado`) y el PDF
permite duplicados en Bronze en reintento ("Bronze en si es
append-only y puede... tener duplicados"), asi que un INSERT directo
por lote desde un proceso Python continuo cumple igual la regla "no
lanzar un Job de Databricks por cada POST; mantener un consumidor
continuo" (el "consumidor continuo" aca es este mismo proceso Python,
no un Job de Databricks) - con muchisima menos infraestructura.

Dos modos, mismo espiritu que Andes:
- drain_topics()/upload_batch(): una sola pasada (pruebas puntuales,
  checkpoints de evidencia).
- loop_continuous(): proceso persistente (mismo patron que
  worker/outbox_worker.py) - poll continuo, batch por tiempo/tamano,
  nunca termina. Es el que debe correr en paralelo al worker outbox
  para que el flujo no se corte en este punto.
"""

import logging
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer

from bridge._env import load_dotenv_if_missing
from bridge.databricks_sql import run_sql

load_dotenv_if_missing()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("novadrive.bridge")

BRONZE_TABLE = "novadrive_catalog.bronze.events"
TOPICS = ["cdc.novadrive.customer", "cdc.novadrive.vehicle", "cdc.novadrive.sale"]

# Mismo hallazgo que Andes documenta en su propio bridge: leer siempre
# KAFKA_BOOTSTRAP_SERVERS del entorno, nunca un default hardcoded que
# solo funcione "por casualidad" porque el bridge corre fuera de Docker.
BOOTSTRAP_SERVERS_DEFAULT = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092")

# Hallazgo real (9 sep 2026): loop_continuous() atrapaba CUALQUIER fallo
# de upload_batch() en un except Exception silencioso, para siempre - el
# buffer nunca se vaciaba pero tampoco dejaba de crecer, y el fallo solo
# era visible si alguien miraba docker logs a proposito. Dos umbrales
# configurables por entorno para no repetir esto:
#   - MAX_CONSECUTIVE_FAILURES: tras esta cantidad de flushes fallidos
#     seguidos, el proceso deja de reintentar en silencio y termina con
#     un log CRITICAL bien visible - ver restart:unless-stopped en
#     docker-compose.yml, que lo reinicia solo (igual que Andes).
#   - MAX_BUFFER_SIZE: tope de mensajes en memoria mientras el flush
#     viene fallando. Al llegar al tope, el bridge deja de hacer poll()
#     (backpressure) en vez de seguir acumulando sin limite - los
#     mensajes no leidos quedan intactos en Redpanda, no se pierden.
MAX_CONSECUTIVE_FAILURES = int(os.environ.get("BRIDGE_MAX_CONSECUTIVE_FAILURES", "8"))
MAX_BUFFER_SIZE = int(os.environ.get("BRIDGE_MAX_BUFFER_SIZE", "2000"))


def _sql_str(value: str) -> str:
    """Escapa un literal STRING para Spark SQL (ANSI: comilla simple
    duplicada). El payload es JSON compacto de una sola linea (ver
    common/events/serialization.py), asi que no hay saltos de linea que
    manejar."""
    return "'" + value.replace("'", "''") + "'"


def _sql_timestamp(dt: datetime) -> str:
    return "TIMESTAMP" + _sql_str(dt.strftime("%Y-%m-%d %H:%M:%S.%f"))


def drain_topics(bootstrap_servers: str = BOOTSTRAP_SERVERS_DEFAULT, idle_timeout_sec: float = 3.0) -> list[dict]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "novadrive-bridge-databricks",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe(TOPICS)

    messages: list[dict] = []
    last_message_at = time.time()
    try:
        while time.time() - last_message_at < idle_timeout_sec:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.warning("consume error: %s", msg.error())
                continue
            messages.append(
                {
                    "kafka_topic": msg.topic(),
                    "kafka_partition": msg.partition(),
                    "kafka_offset": msg.offset(),
                    "kafka_timestamp": datetime.fromtimestamp(msg.timestamp()[1] / 1000, tz=timezone.utc),
                    "ingested_at": datetime.now(timezone.utc),
                    "payload": msg.value().decode("utf-8"),
                }
            )
            last_message_at = time.time()
    finally:
        consumer.close()

    return messages


def upload_batch(messages: list[dict], batch_size: int = 50) -> int:
    """INSERT directo a Bronze, en lotes de `batch_size` filas por
    statement (limite prudente, no un limite documentado de la API).
    Devuelve la cantidad de filas insertadas."""
    if not messages:
        return 0

    inserted = 0
    for i in range(0, len(messages), batch_size):
        chunk = messages[i : i + batch_size]
        values_sql = ",\n".join(
            "({}, {}, {}, {}, {}, {}, NULL)".format(
                _sql_str(m["kafka_topic"]),
                m["kafka_partition"],
                m["kafka_offset"],
                _sql_timestamp(m["kafka_timestamp"]),
                _sql_timestamp(m["ingested_at"]),
                _sql_str(m["payload"]),
            )
            for m in chunk
        )
        stmt = (
            f"INSERT INTO {BRONZE_TABLE} "
            "(kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, ingested_at, payload, source_file)\n"
            f"VALUES\n{values_sql}"
        )
        result = run_sql(stmt)
        if result["status"]["state"] != "SUCCEEDED":
            raise RuntimeError(f"Bronze insert failed: {result['status'].get('error')}")
        inserted += len(chunk)

    return inserted


def main() -> None:
    print("[BRIDGE] Draining NovaDrive Redpanda topics (cdc.novadrive.customer/vehicle/sale)...")
    messages = drain_topics()
    print(f"[BRIDGE] {len(messages)} messages read from Redpanda.")
    inserted = upload_batch(messages)
    print(f"[BRIDGE] {inserted} rows inserted into {BRONZE_TABLE}.")


def loop_continuous(
    bootstrap_servers: str = BOOTSTRAP_SERVERS_DEFAULT,
    flush_interval_sec: float = 5.0,
    max_batch_size: int = 200,
) -> None:
    """Proceso persistente: nunca termina en operacion normal. Mismo
    espiritu que worker/outbox_worker.py::loop() - poll, acumular, subir
    cuando corresponda por tiempo o tamano, repetir. Este es el
    "consumidor continuo" que exige la seccion 4 del PDF (nunca un Job
    de Databricks por evento).

    Manejo de fallos (ver MAX_CONSECUTIVE_FAILURES/MAX_BUFFER_SIZE mas
    arriba): un flush fallido no se reintenta en silencio para siempre -
    tras el umbral, el proceso termina con un log CRITICAL para que
    restart:unless-stopped lo reinicie (visible en docker ps/docker
    logs, igual que el bridge de Andes). El buffer tiene tope (backpressure,
    no crece sin limite).

    `enable.auto.commit` en False, a proposito - hallazgo encontrado al
    implementar el punto anterior: con auto-commit en True, Kafka podia
    dar por consumidos mensajes que todavia estaban solo en el buffer en
    memoria, sin haber llegado a Bronze; si el proceso terminaba (antes,
    por una excepcion no atrapada; ahora, a proposito tras el umbral de
    reintentos) esos mensajes se perdian para siempre en el proximo
    arranque en vez de reintentarse - contradice el contrato at-least-once
    de CLAUDE.md. Ahora el commit de offsets ocurre recien despues de un
    flush exitoso: en el peor caso, un reinicio reprocesa el mismo lote
    (duplicados en Bronze, esperados y aceptados por el contrato), nunca
    lo pierde."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "novadrive-bridge-databricks",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(TOPICS)
    logger.info(
        "NovaDrive bridge started (continuous loop). topics=%s -> %s max_consecutive_failures=%s max_buffer_size=%s",
        TOPICS, BRONZE_TABLE, MAX_CONSECUTIVE_FAILURES, MAX_BUFFER_SIZE,
    )

    buffer: list[dict] = []
    last_flush = time.time()
    consecutive_failures = 0
    try:
        while True:
            # Backpressure: si el buffer esta al tope (el flush viene
            # fallando), dejamos de leer de Redpanda - los mensajes no
            # leidos quedan intactos en el broker, no se pierden.
            if len(buffer) < MAX_BUFFER_SIZE:
                msg = consumer.poll(timeout=1.0)
            else:
                msg = None
                time.sleep(1.0)

            if msg is not None:
                if msg.error():
                    logger.warning("consume error: %s", msg.error())
                else:
                    buffer.append(
                        {
                            "kafka_topic": msg.topic(),
                            "kafka_partition": msg.partition(),
                            "kafka_offset": msg.offset(),
                            "kafka_timestamp": datetime.fromtimestamp(msg.timestamp()[1] / 1000, tz=timezone.utc),
                            "ingested_at": datetime.now(timezone.utc),
                            "payload": msg.value().decode("utf-8"),
                        }
                    )

            should_flush = buffer and (
                len(buffer) >= max_batch_size
                or time.time() - last_flush >= flush_interval_sec
                or len(buffer) >= MAX_BUFFER_SIZE
            )
            if should_flush:
                try:
                    n = upload_batch(buffer)
                    logger.info("Flushed %s events to %s", n, BRONZE_TABLE)
                except Exception as exc:
                    consecutive_failures += 1
                    logger.exception(
                        "Failed to flush batch to Bronze (%s/%s consecutive failures) - keeping %s buffered events for retry",
                        consecutive_failures, MAX_CONSECUTIVE_FAILURES, len(buffer),
                    )
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        logger.critical(
                            "CRITICAL: bridge failed to flush to Bronze %s times in a row (last error: %s) - "
                            "giving up so the container exits and restart:unless-stopped brings it back. "
                            "%s events remain unflushed; nothing is lost, offsets are only committed after a "
                            "successful flush, so they will be re-read from Redpanda after restart.",
                            consecutive_failures, exc, len(buffer),
                        )
                        raise
                    time.sleep(2.0)
                    continue
                # Flush exitoso: recien ahora commiteamos los offsets de
                # lo que efectivamente llego a Bronze (ver docstring).
                consumer.commit(asynchronous=False)
                buffer = []
                last_flush = time.time()
                consecutive_failures = 0
    finally:
        consumer.close()


if __name__ == "__main__":
    mode = os.environ.get("BRIDGE_MODE", "once")
    if mode == "loop":
        loop_continuous()
    else:
        main()

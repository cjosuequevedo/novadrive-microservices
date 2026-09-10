"""
bridge_redpanda_to_databricks.py - Puente Redpanda (propio de NovaDrive)
-> Volume de Unity Catalog `novadrive_catalog.bronze.raw_events`
(Databricks). Consume los 3 topicos de NovaDrive, enriquece con
metadatos Kafka reales (topic/partition/offset/timestamp) y sube cada
batch como UN SOLO ARCHIVO .jsonl via la Files API - EXACTAMENTE el
mismo patron que usa Andes en su propio bridge
(`andes-motors/scripts/bridge_redpanda_to_databricks.py::subir_batch()`).

CAMBIO DE ARQUITECTURA (9 sep 2026) - revierte la decision original de
este archivo: hasta hoy este bridge hacia INSERT SQL directo a una
tabla `novadrive_catalog.bronze.events`, decision tomada a proposito
porque NovaDrive no tiene Silver y no necesitaba las garantias de
Auto Loader que Andes si necesita. Esa razon tecnica seguia siendo
valida, pero el equipo de trabajo pidio explicitamente (requisito no
negociable) que ambos sistemas hermanos suban los datos crudos de la
MISMA manera para consistencia entre proyectos - confirmado por CJ
tras señalarle la contradiccion con la decision anterior, no aplicado
en silencio. La tabla SQL vieja (`bronze.events`) se elimino: ya no
existe ninguna tabla Delta en este punto del pipeline, solo el Volume
con archivos crudos - convertir esos archivos en tabla (Auto Loader)
es un paso posterior y separado, fuera de alcance salvo que se pida
explicitamente (igual criterio que en Andes).

Dos modos, mismo espiritu que Andes:
- drain_topics()/upload_batch(): una sola pasada (pruebas puntuales,
  checkpoints de evidencia).
- loop_continuous(): proceso persistente (mismo patron que
  worker/outbox_worker.py) - poll continuo, batch por tiempo/tamano,
  nunca termina. Es el que debe correr en paralelo al worker outbox
  para que el flujo no se corte en este punto.

Lo que SI se conserva de la version anterior, a proposito - no es
parte de lo que Andes pidio igualar, son mejoras de confiabilidad
propias de NovaDrive, ortogonales a "SQL vs archivos" (ver hallazgos
tecnicos #11/#12 en CLAUDE.md): `enable.auto.commit=False` + commit
manual solo tras una subida exitosa (evita perder eventos si el
proceso termina a mitad de camino), y el umbral de
MAX_CONSECUTIVE_FAILURES/MAX_BUFFER_SIZE con backpressure. Andes no
tiene estas dos protecciones en su propio bridge; no hay pedido de
quitarlas aca, y quitarlas reintroduciria un problema real ya
resuelto.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer

from bridge._env import load_dotenv_if_missing
from bridge.databricks_files import upload_file

load_dotenv_if_missing()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("novadrive.bridge")

CATALOG = "novadrive_catalog"
SCHEMA = "bronze"
VOLUME = "raw_events"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
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


def upload_batch(messages: list[dict]) -> str | None:
    """Sube TODO el batch como UN SOLO archivo .jsonl al Volume (mismo
    patron que `subir_batch()` en Andes) - ya no hace INSERT SQL, ver
    docstring del modulo. Devuelve el nombre del archivo subido, o None
    si no habia nada que subir.

    Convencion de nombre: `novadrive_batch_<YYYYMMDD>_<HHMMSS>_<microsegundos>.jsonl`
    - mismo formato que Andes (`batch_...`), prefijo `novadrive_` para
      poder distinguir el origen si algun dia ambos Volumes se miran
      juntos."""
    if not messages:
        return None

    filename = f"novadrive_batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.jsonl"
    lines = []
    for m in messages:
        record = {
            "kafka_topic": m["kafka_topic"],
            "kafka_partition": m["kafka_partition"],
            "kafka_offset": m["kafka_offset"],
            "kafka_timestamp": m["kafka_timestamp"].isoformat(),
            "ingested_at": m["ingested_at"].isoformat(),
            "payload": m["payload"],
            "source_file": filename,
        }
        lines.append(json.dumps(record, ensure_ascii=False))
    content = "\n".join(lines).encode("utf-8")

    upload_file(f"{VOLUME_PATH}/{filename}", content)
    return filename


def main() -> None:
    print("[BRIDGE] Draining NovaDrive Redpanda topics (cdc.novadrive.customer/vehicle/sale)...")
    messages = drain_topics()
    print(f"[BRIDGE] {len(messages)} messages read from Redpanda.")
    filename = upload_batch(messages)
    if filename:
        print(f"[BRIDGE] Uploaded to {VOLUME_PATH}/{filename} ({len(messages)} events).")
    else:
        print("[BRIDGE] Nothing to upload (no pending messages).")


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
        TOPICS, VOLUME_PATH, MAX_CONSECUTIVE_FAILURES, MAX_BUFFER_SIZE,
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
                    filename = upload_batch(buffer)
                    logger.info("Uploaded %s to %s/%s", len(buffer), VOLUME_PATH, filename)
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

"""
outbox_worker.py - Worker outbox NovaDrive (seccion 13 del documento).
Proceso unico, compartido por los tres microservicios (ver CLAUDE.md,
decision #3) - drena `nd.outbox_event` sin importar cual de los tres
escribio cada fila.

Protocolo:
1. Buscar lote pequeno de PENDING/RETRY cuyo NEXT_RETRY_AT ya vencio.
2. Reclamar filas con FOR UPDATE SKIP LOCKED y pasarlas a PROCESSING.
3. Publicar topic, kafka key y payload y esperar confirmacion.
4. Marcar PUBLISHED y completar PUBLISHED_AT cuando Redpanda confirme.
5. Ante error, incrementar ATTEMPTS, guardar LAST_ERROR y aplicar backoff.
6. Tras el maximo de intentos, marcar FAILED (reactivacion manual, fuera
   de alcance del MVP).

Diferencia real con Andes (Oracle): el reclamo es de UNA SOLA consulta
`SELECT ... FOR UPDATE SKIP LOCKED LIMIT n`, no el patron de dos pasos
que exige Oracle (ORA-02014). Verificado con evidencia real de
concurrencia en la Fase 1 (ver CLAUDE.md, seccion "Worker outbox") -
no se porta el patron de dos pasos de Andes sin necesidad.

Nota de diseno (igual que Andes): reclamar_lote() extrae los campos
necesarios a dicts ANTES de hacer commit() y devuelve esos dicts, no
los objetos ORM - evita DetachedInstanceError al acceder a los
atributos despues de cerrar esa sesion.
"""

import logging
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from sqlalchemy import and_, or_, select

from common.config import get_settings
from common.db import SessionLocal
from common.models.outbox_event import OutboxEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("novadrive.outbox_worker")

settings = get_settings()


def _build_producer() -> Producer:
    conf: dict = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
    }
    if settings.kafka_security_protocol and settings.kafka_security_protocol != "PLAINTEXT":
        conf["security.protocol"] = settings.kafka_security_protocol
    if settings.kafka_sasl_mechanism:
        conf["sasl.mechanism"] = settings.kafka_sasl_mechanism
        conf["sasl.username"] = settings.kafka_sasl_username
        conf["sasl.password"] = settings.kafka_sasl_password
    return Producer(conf)


def claim_batch(session, limit: int) -> list[dict]:
    """Una sola consulta - Postgres SI soporta FOR UPDATE + LIMIT juntos
    (a diferencia de Oracle), verificado con dos transacciones
    concurrentes reales en la Fase 1 (ver CLAUDE.md).

    Incluye tambien filas PROCESSING "viejas" (claimed_at anterior al
    umbral de STALE_PROCESSING_SEC) - visibility timeout tipo cola de
    mensajes. Sin esto, una fila que un worker reclamo pero nunca llego
    a marcar PUBLISHED/RETRY/FAILED (proceso matado a mitad de ciclo,
    contenedor destruido, crash no capturado) queda en PROCESSING para
    siempre: el WHERE original solo mira PENDING/RETRY, asi que ninguna
    corrida futura la vuelve a tomar. Encontrado con evidencia real en
    este mismo checkpoint (9 sep 2026): un contenedor de prueba se
    destruyo entre el claim y el registro del resultado, dejando un
    evento real trabado en PROCESSING de forma permanente hasta este
    fix - ver CLAUDE.md."""
    now = datetime.now(timezone.utc)
    stale_before = now - timedelta(seconds=settings.stale_processing_sec)

    stmt = (
        select(OutboxEvent)
        .where(
            or_(
                and_(OutboxEvent.status.in_(["PENDING", "RETRY"]), OutboxEvent.next_retry_at <= now),
                and_(OutboxEvent.status == "PROCESSING", OutboxEvent.claimed_at <= stale_before),
            )
        )
        .order_by(OutboxEvent.created_at)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    rows = list(session.execute(stmt).scalars())
    if not rows:
        return []

    claimed = []
    for row in rows:
        row.status = "PROCESSING"
        row.claimed_at = now
        claimed.append(
            {
                "outbox_id": row.outbox_id,
                "event_id": row.event_id,
                "entity_name": row.entity_name,
                "operation": row.operation,
                "kafka_topic": row.kafka_topic,
                "kafka_key": row.kafka_key,
                "payload_json": row.payload_json,
            }
        )

    session.commit()
    return claimed


def publish_one(producer: Producer, row: dict) -> dict:
    """Publica y espera confirmacion (sincrona via flush) o el error del broker."""
    result: dict = {}

    def _callback(err, msg):
        if err is not None:
            result["error"] = str(err)
        else:
            result["topic"] = msg.topic()
            result["partition"] = msg.partition()
            result["offset"] = msg.offset()

    producer.produce(
        topic=row["kafka_topic"],
        key=row["kafka_key"].encode("utf-8"),
        value=row["payload_json"].encode("utf-8"),
        callback=_callback,
    )
    pending = producer.flush(10)
    if pending > 0 and "error" not in result:
        result["error"] = "timeout waiting for broker confirmation"
    return result


def run_cycle(producer: Producer | None = None) -> int:
    producer = producer or _build_producer()

    session = SessionLocal()
    try:
        rows = claim_batch(session, settings.outbox_batch_size)
    finally:
        session.close()

    if not rows:
        return 0

    published = 0
    for row in rows:
        # Defensa en profundidad ademas del visibility timeout de
        # claim_batch: si publish_one() revienta de forma sincrona (ej.
        # BufferError de confluent-kafka con la cola local llena, o
        # cualquier excepcion no capturada por el callback de entrega),
        # que la fila quede marcada RETRY de inmediato en vez de
        # depender solo de que pase el umbral de staleness.
        try:
            result = publish_one(producer, row)
        except Exception as exc:  # noqa: BLE001 - cualquier fallo del cliente Kafka cuenta como error de publicacion
            result = {"error": f"{type(exc).__name__}: {exc}"}

        s2 = SessionLocal()
        try:
            db_row = s2.get(OutboxEvent, row["outbox_id"])
            if "error" in result:
                db_row.attempts += 1
                db_row.last_error = result["error"][:1000]
                if db_row.attempts >= settings.outbox_max_retries:
                    db_row.status = "FAILED"
                    logger.error(
                        "event_id=%s FAILED after %s attempts: %s",
                        db_row.event_id, db_row.attempts, result["error"],
                    )
                else:
                    db_row.status = "RETRY"
                    backoff_sec = min(60, 2 ** db_row.attempts)
                    db_row.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=backoff_sec)
                    logger.warning(
                        "event_id=%s RETRY attempt=%s backoff=%ss: %s",
                        db_row.event_id, db_row.attempts, backoff_sec, result["error"],
                    )
            else:
                db_row.status = "PUBLISHED"
                db_row.published_at = datetime.now(timezone.utc)
                published += 1
                logger.info(
                    "PUBLISHED event_id=%s entity=%s op=%s topic=%s partition=%s offset=%s",
                    db_row.event_id, db_row.entity_name, db_row.operation,
                    result.get("topic"), result.get("partition"), result.get("offset"),
                )
            s2.commit()
        finally:
            s2.close()

    return published


def loop(interval_sec: float | None = None) -> None:
    interval = interval_sec if interval_sec is not None else settings.outbox_poll_ms / 1000
    logger.info(
        "NovaDrive outbox worker started. bootstrap=%s interval=%ss batch=%s",
        settings.kafka_bootstrap_servers, interval, settings.outbox_batch_size,
    )
    while True:
        n = run_cycle()
        if n == 0:
            time.sleep(interval)


if __name__ == "__main__":
    loop()

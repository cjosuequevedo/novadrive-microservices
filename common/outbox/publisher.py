"""
publisher.py - Registra un EventEnvelope en nd.outbox_event.

Regla no negociable (secciones 4 y 9 del documento): esta funcion
NUNCA hace session.commit(). Solo agrega la fila a la sesion
(session.add) para que el llamador la confirme en el MISMO commit que
el cambio de negocio (INSERT/UPDATE/DELETE en nd.customer/
nd.inventory_unit/nd.sales_order). Si el llamador hace rollback, este
evento se revierte junto con el negocio - esa atomicidad es el patron
outbox completo. Compartida por los tres microservicios (todos
escriben a la misma tabla `nd.outbox_event`, ver CLAUDE.md decision #2).
"""

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from common.events.envelope import EventEnvelope
from common.events.serialization import dumps_decimal_safe
from common.events.topics import kafka_key_for, topic_for
from common.models.outbox_event import OutboxEvent


def enqueue_outbox_event(
    session: Session,
    envelope: EventEnvelope,
    *,
    record_key_value: object,
) -> OutboxEvent:
    """Construye la fila nd.outbox_event a partir del envelope y la
    agrega a `session` (sin commit). Devuelve el objeto ORM agregado
    para que el llamador pueda inspeccionarlo (ej. leer event_id) antes
    de confirmar la transaccion.

    `record_key_value` es el valor del codigo de negocio (ej. el
    CUSTOMER_CODE) usado para construir la kafka_key estable de
    particion - separado de `envelope.record_key` (que es el dict
    completo, pensado para RECORD_KEY_JSON) para no asumir cual campo
    del dict es la clave.
    """
    ahora = datetime.now(timezone.utc)

    fila = OutboxEvent(
        event_id=envelope.event_id,
        event_version=envelope.event_version,
        source_system=envelope.source_system,
        entity_name=envelope.entity,
        source_table=envelope.source_table,
        operation=envelope.operation,
        record_key_json=dumps_decimal_safe(envelope.record_key),
        kafka_topic=topic_for(envelope.entity),
        kafka_key=kafka_key_for(envelope.entity, record_key_value),
        payload_json=dumps_decimal_safe(envelope.to_contract_dict()),
        status="PENDING",
        attempts=0,
        next_retry_at=ahora,
        occurred_at=envelope.occurred_at,
        created_at=ahora,
        correlation_id=envelope.correlation_id,
    )
    session.add(fila)
    return fila

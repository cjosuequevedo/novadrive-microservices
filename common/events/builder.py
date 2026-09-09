"""
builder.py - Construye un EventEnvelope antes/despues valido.

Reglas de la tabla "Operacion / before / after" (seccion 11):
    c -> before=None,        after=snapshot nuevo
    u -> before=anterior,    after=snapshot nuevo
    d -> before=eliminado,   after=None

Identico a Andes (contrato compartido) - la unica diferencia real es
que `ENTITY_CONFIG`/`SOURCE_SYSTEM` vienen de common.events.topics con
los nombres NovaDrive.
"""

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from common.events.envelope import EventEnvelope, Operation
from common.events.topics import Entity, ENTITY_CONFIG, source_table_for


class EventContractError(ValueError):
    """El before/after no respeta la tabla de la seccion 11 para la operacion dada."""


def _validar_before_after(operation: Operation, before: dict[str, Any] | None, after: dict[str, Any] | None) -> None:
    if operation == "c":
        if before is not None:
            raise EventContractError("operation='c' (creacion) exige before=None")
        if after is None:
            raise EventContractError("operation='c' (creacion) exige after con el snapshot nuevo")
    elif operation == "u":
        if before is None or after is None:
            raise EventContractError("operation='u' (actualizacion) exige before Y after presentes")
    elif operation == "d":
        if after is not None:
            raise EventContractError("operation='d' (eliminacion) exige after=None")
        if before is None:
            raise EventContractError("operation='d' (eliminacion) exige before con el snapshot eliminado")
    else:
        raise EventContractError(f"operation invalida: {operation!r} (debe ser 'c', 'u' o 'd')")


def build_event(
    *,
    entity: Entity,
    operation: Operation,
    record_key: dict[str, Any],
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    correlation_id: str,
    occurred_at: datetime | None = None,
) -> EventEnvelope:
    """Construye y valida un EventEnvelope. No toca la base de datos ni Kafka.

    `record_key` es la clave de negocio serializada (ej.
    {"CUSTOMER_CODE": "CUS0001"}), no el snapshot completo.
    """
    if entity not in ENTITY_CONFIG:
        raise EventContractError(f"entidad desconocida: {entity!r} (debe ser customer, vehicle o sale)")

    _validar_before_after(operation, before, after)

    return EventEnvelope(
        event_id=str(uuid4()),
        event_version=1,
        entity=entity,
        source_table=source_table_for(entity),
        operation=operation,
        record_key=record_key,
        before=before,
        after=after,
        occurred_at=occurred_at or datetime.now(timezone.utc),
        correlation_id=correlation_id,
    )

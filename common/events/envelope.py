"""
envelope.py - Contrato de eventos (seccion 11 del documento), como
modelo Pydantic. Mismo contrato exacto que usa Andes (compartido entre
los dos sistemas, ver CLAUDE.md "Contrato de eventos") - unica
diferencia real es `source_system` fijo en "NOVADRIVE".

Campo por campo, tal como la tabla "Contrato de eventos":
event_id, event_version, source_system, entity, source_table,
operation (c/u/d), record_key, before, after, occurred_at,
correlation_id.
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from common.events.topics import Entity, SOURCE_SYSTEM

Operation = Literal["c", "u", "d"]


class EventEnvelope(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: str
    event_version: int = 1
    source_system: Literal["NOVADRIVE"] = SOURCE_SYSTEM  # type: ignore[assignment]
    entity: Entity
    source_table: str
    operation: Operation
    record_key: dict[str, Any]
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    occurred_at: datetime
    correlation_id: str

    def to_contract_dict(self) -> dict[str, Any]:
        """Dict Python (no JSON todavia) con exactamente los campos del
        contrato, en el orden de la seccion 11 - lo que va serializado
        en PAYLOAD_JSON."""
        return {
            "event_id": self.event_id,
            "event_version": self.event_version,
            "source_system": self.source_system,
            "entity": self.entity,
            "source_table": self.source_table,
            "operation": self.operation,
            "record_key": self.record_key,
            "before": self.before,
            "after": self.after,
            "occurred_at": self.occurred_at,
            "correlation_id": self.correlation_id,
        }

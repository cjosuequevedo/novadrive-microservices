"""Schemas de respuesta compartidos por los tres microservicios
(Customer, Inventory, Order) - la forma es la misma en los tres casos
(seccion 8: "Cada respuesta debe mostrar correlation_id y los event_id
generados"), asi que vive en un solo lugar del paquete `common` en vez
de repetirse en cada servicio."""

from typing import Any

from pydantic import BaseModel


class EventRef(BaseModel):
    event_id: str
    entity: str
    operation: str


class MutationResponse(BaseModel):
    correlation_id: str
    events: list[EventRef]
    data: dict[str, Any] | None = None

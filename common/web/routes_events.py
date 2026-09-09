"""GET /events/{event_id}/status - consulta de estado del outbox
(regla explicita de CLAUDE.md / seccion 8 del documento).

Compartido por los tres microservicios: los tres escriben a la misma
tabla `nd.outbox_event`, asi que cualquiera de ellos puede resolver el
estado de cualquier event_id, sin importar cual servicio lo genero.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from common.db import get_session
from common.models.outbox_event import OutboxEvent
from common.security import require_auth

router = APIRouter(dependencies=[Depends(require_auth)])


@router.get("/events/{event_id}/status")
def event_status(event_id: str, session: Session = Depends(get_session)) -> dict:
    event = session.query(OutboxEvent).filter_by(event_id=event_id).one_or_none()
    if event is None:
        raise HTTPException(status_code=404, detail=f"event_id {event_id} not found")

    return {
        "event_id": event.event_id,
        "entity": event.entity_name,
        "operation": event.operation,
        "status": event.status,
        "attempts": event.attempts,
        "next_retry_at": event.next_retry_at,
        "published_at": event.published_at,
        "last_error": event.last_error,
        "correlation_id": event.correlation_id,
    }

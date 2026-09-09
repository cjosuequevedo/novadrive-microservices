"""
service.py - Reglas transaccionales de negocio para Inventory (seccion 9
del documento).

Aqui `chassis_number` ES la PK (a diferencia de Andes, donde VIN es un
campo unico pero VEHICULO_ID es la PK autogenerada aparte) - por eso no
hay generacion de codigo como en Customer: el chasis lo escribe el
usuario y su unicidad la garantiza la PK misma (mas el prechequeo, para
un mensaje de error mas claro que un IntegrityError crudo)."""

from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from common.events.builder import build_event
from common.models.inventory_unit import InventoryUnit
from common.models.outbox_event import OutboxEvent
from common.outbox.publisher import enqueue_outbox_event
from common.errors import ConflictError, NotFoundError, ValidationDomainError
from services.inventory import repository
from services.inventory.schemas import InventoryEntrada


def _snapshot(unit: InventoryUnit) -> dict[str, Any]:
    return {
        "CHASSIS_NUMBER": unit.chassis_number,
        "BRANCH_CODE": unit.branch_code,
        "BRAND_NAME": unit.brand_name,
        "MODEL_NAME": unit.model_name,
        "MODEL_YEAR": unit.model_year,
        "EXTERIOR_COLOUR": unit.exterior_colour,
        "LIST_AMOUNT": unit.list_amount,
        "AVAILABILITY_CODE": unit.availability_code,
    }


def _require_branch(session: Session, branch_code: str) -> None:
    if not repository.branch_exists_and_enabled(session, branch_code):
        raise ValidationDomainError(f"Branch {branch_code} does not exist or is not enabled")


def create_inventory_unit(
    session: Session, payload: InventoryEntrada, correlation_id: str
) -> tuple[InventoryUnit, list[OutboxEvent]]:
    _require_branch(session, payload.branch_code)

    if repository.get(session, payload.chassis_number) is not None:
        raise ConflictError(f"An inventory unit with chassis {payload.chassis_number} already exists")

    unit = InventoryUnit(
        chassis_number=payload.chassis_number,
        branch_code=payload.branch_code,
        brand_name=payload.brand_name,
        model_name=payload.model_name,
        model_year=payload.model_year,
        exterior_colour=payload.exterior_colour,
        list_amount=payload.list_amount,
        availability_code=payload.availability_code,
    )
    session.add(unit)
    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        raise ConflictError("Duplicate chassis_number (pk_inventory_unit constraint)") from exc

    after = _snapshot(unit)
    event = build_event(
        entity="vehicle",
        operation="c",
        record_key={"CHASSIS_NUMBER": unit.chassis_number},
        before=None,
        after=after,
        correlation_id=correlation_id,
    )
    outbox_row = enqueue_outbox_event(session, event, record_key_value=unit.chassis_number)

    session.commit()  # ONE commit: business row + outbox row together
    session.refresh(unit)
    return unit, [outbox_row]


def edit_inventory_unit(
    session: Session, chassis_number: str, payload: InventoryEntrada, correlation_id: str
) -> tuple[InventoryUnit, list[OutboxEvent]]:
    unit = repository.get_for_update(session, chassis_number)  # FOR UPDATE
    if unit is None:
        raise NotFoundError(f"Inventory unit {chassis_number} does not exist")

    before = _snapshot(unit)

    if payload.chassis_number != chassis_number:
        raise ValidationDomainError("chassis_number cannot be changed on edit; it is the primary key")

    _require_branch(session, payload.branch_code)

    unit.branch_code = payload.branch_code
    unit.brand_name = payload.brand_name
    unit.model_name = payload.model_name
    unit.model_year = payload.model_year
    unit.exterior_colour = payload.exterior_colour
    unit.list_amount = payload.list_amount
    unit.availability_code = payload.availability_code

    session.flush()

    after = _snapshot(unit)
    event = build_event(
        entity="vehicle",
        operation="u",
        record_key={"CHASSIS_NUMBER": chassis_number},
        before=before,
        after=after,
        correlation_id=correlation_id,
    )
    outbox_row = enqueue_outbox_event(session, event, record_key_value=chassis_number)

    session.commit()
    session.refresh(unit)
    return unit, [outbox_row]


def delete_inventory_unit(session: Session, chassis_number: str, correlation_id: str) -> list[OutboxEvent]:
    unit = repository.get_for_update(session, chassis_number)  # FOR UPDATE
    if unit is None:
        raise NotFoundError(f"Inventory unit {chassis_number} does not exist")

    # Business rule (section 9): "No permitir ... eliminar unidades ya
    # vinculadas a una orden."
    if repository.is_in_an_order(session, chassis_number):
        raise ConflictError(f"Inventory unit {chassis_number} already belongs to an order; cannot be deleted")

    before = _snapshot(unit)
    session.delete(unit)
    session.flush()

    event = build_event(
        entity="vehicle",
        operation="d",
        record_key={"CHASSIS_NUMBER": chassis_number},
        before=before,
        after=None,
        correlation_id=correlation_id,
    )
    outbox_row = enqueue_outbox_event(session, event, record_key_value=chassis_number)

    session.commit()
    return [outbox_row]

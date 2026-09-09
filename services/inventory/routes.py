"""Rutas de Inventory - alta, edicion, eliminacion (seccion 8: POST
/inventory, POST /inventory/{chassis}/edit, POST /inventory/{chassis}/delete)."""

from decimal import Decimal, InvalidOperation
from uuid import uuid4

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse
from pydantic import ValidationError
from sqlalchemy.orm import Session

from common.db import get_session
from common.errors import ConflictError, NotFoundError, ValidationDomainError
from common.schemas.common import EventRef, MutationResponse
from common.security import require_auth
from services.inventory import repository, service
from services.inventory.schemas import BranchOption, InventoryEntrada, InventoryOut
from services.inventory.web import templates

router = APIRouter(dependencies=[Depends(require_auth)])


def _build_payload(
    branch_code: str,
    chassis_number: str,
    brand_name: str,
    model_name: str,
    model_year: str,
    exterior_colour: str | None,
    list_amount: str,
    availability_code: str,
) -> InventoryEntrada:
    try:
        parsed_amount = Decimal(list_amount)
    except InvalidOperation:
        raise HTTPException(
            status_code=422,
            detail=[{"loc": ["list_amount"], "msg": f"{list_amount!r} is not a valid decimal amount"}],
        )
    try:
        return InventoryEntrada(
            branch_code=branch_code,
            chassis_number=chassis_number,
            brand_name=brand_name,
            model_name=model_name,
            model_year=int(model_year),
            exterior_colour=exterior_colour or None,
            list_amount=parsed_amount,
            availability_code=availability_code,  # type: ignore[arg-type]
        )
    except ValidationError as exc:
        # jsonable_encoder, no exc.errors() a secas - ver hallazgo #13 de
        # Andes (un Decimal invalido en el detalle revienta el
        # JSONResponse por defecto con un 500 en vez del 422 esperado).
        raise HTTPException(status_code=422, detail=jsonable_encoder(exc.errors()))


def _response(correlation_id: str, events, unit) -> MutationResponse:
    return MutationResponse(
        correlation_id=correlation_id,
        events=[EventRef(event_id=e.event_id, entity=e.entity_name, operation=e.operation) for e in events],
        data=InventoryOut.model_validate(unit).model_dump(mode="json") if unit is not None else None,
    )


@router.get("/inventory", response_class=HTMLResponse)
def inventory_page(request: Request, session: Session = Depends(get_session)):
    units = repository.list_inventory(session)
    branches = [BranchOption.model_validate(b) for b in repository.list_enabled_branches(session)]
    return templates.TemplateResponse(
        "inventory.html",
        {"request": request, "units": units, "branches": branches, "active_nav": "inventory"},
    )


@router.post("/inventory")
def create_inventory(
    branch_code: str = Form(...),
    chassis_number: str = Form(...),
    brand_name: str = Form(...),
    model_name: str = Form(...),
    model_year: str = Form(...),
    exterior_colour: str | None = Form(None),
    list_amount: str = Form(...),
    availability_code: str = Form("AVL"),
    session: Session = Depends(get_session),
) -> MutationResponse:
    correlation_id = str(uuid4())
    payload = _build_payload(
        branch_code, chassis_number, brand_name, model_name, model_year, exterior_colour, list_amount, availability_code
    )
    try:
        unit, events = service.create_inventory_unit(session, payload, correlation_id)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ValidationDomainError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    return _response(correlation_id, events, unit)


@router.post("/inventory/{chassis_number}/edit")
def edit_inventory(
    chassis_number: str,
    # Leido tambien del cuerpo del formulario (no solo el path) a
    # proposito: asi el guard "no se puede cambiar la PK" en
    # service.edit_inventory_unit compara dos valores realmente
    # independientes, en vez de comparar el path contra si mismo (bug
    # real encontrado en este mismo checkpoint - ver CLAUDE.md).
    chassis_number_body: str = Form(..., alias="chassis_number"),
    branch_code: str = Form(...),
    brand_name: str = Form(...),
    model_name: str = Form(...),
    model_year: str = Form(...),
    exterior_colour: str | None = Form(None),
    list_amount: str = Form(...),
    availability_code: str = Form("AVL"),
    session: Session = Depends(get_session),
) -> MutationResponse:
    correlation_id = str(uuid4())
    payload = _build_payload(
        branch_code, chassis_number_body, brand_name, model_name, model_year, exterior_colour, list_amount, availability_code
    )
    try:
        unit, events = service.edit_inventory_unit(session, chassis_number, payload, correlation_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ValidationDomainError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    return _response(correlation_id, events, unit)


@router.post("/inventory/{chassis_number}/delete")
def delete_inventory(chassis_number: str, session: Session = Depends(get_session)) -> MutationResponse:
    correlation_id = str(uuid4())
    try:
        events = service.delete_inventory_unit(session, chassis_number, correlation_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    return _response(correlation_id, events, None)

"""Rutas de Order - alta, edicion, eliminacion (seccion 8: POST
/orders, POST /orders/{number}/edit, POST /orders/{number}/delete)."""

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
from services.orders import repository, service
from services.orders.schemas import (
    AgentOption,
    BranchOption,
    CustomerOption,
    InventoryOption,
    OrderEdicion,
    OrderEntrada,
    OrderOut,
)
from services.orders.web import templates

router = APIRouter(dependencies=[Depends(require_auth)])


def _parse_decimal(field_name: str, raw: str | None, required: bool) -> Decimal | None:
    if raw is None or raw == "":
        if required:
            raise HTTPException(status_code=422, detail=[{"loc": [field_name], "msg": "field required"}])
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        raise HTTPException(status_code=422, detail=[{"loc": [field_name], "msg": f"{raw!r} is not a valid decimal"}])


def _build_payload(
    buyer_code: str,
    sales_agent_code: str,
    fulfillment_branch: str,
    chassis_number: str,
    unit_gross_amount: str | None,
    discount_pct: str,
    tax_amount: str,
    currency_code: str,
    payment_channel: str | None,
    captured_amount: str | None,
    payment_reference: str | None,
) -> OrderEntrada:
    try:
        return OrderEntrada(
            buyer_code=buyer_code,
            sales_agent_code=sales_agent_code,
            fulfillment_branch=fulfillment_branch,
            chassis_number=chassis_number,
            unit_gross_amount=_parse_decimal("unit_gross_amount", unit_gross_amount, required=False),
            discount_pct=_parse_decimal("discount_pct", discount_pct, required=True),
            tax_amount=_parse_decimal("tax_amount", tax_amount, required=True),
            currency_code=currency_code,
            payment_channel=payment_channel or None,
            captured_amount=_parse_decimal("captured_amount", captured_amount, required=False),
            payment_reference=payment_reference or None,
        )
    except ValidationError as exc:
        # jsonable_encoder, no exc.errors() a secas - ver hallazgo #13 de
        # Andes (un Decimal invalido en el detalle revienta el
        # JSONResponse por defecto con un 500 en vez del 422 esperado).
        raise HTTPException(status_code=422, detail=jsonable_encoder(exc.errors()))


def _response(correlation_id: str, events, order) -> MutationResponse:
    return MutationResponse(
        correlation_id=correlation_id,
        events=[EventRef(event_id=e.event_id, entity=e.entity_name, operation=e.operation) for e in events],
        data=OrderOut.model_validate(order).model_dump(mode="json") if order is not None else None,
    )


@router.get("/orders", response_class=HTMLResponse)
def orders_page(request: Request, session: Session = Depends(get_session)):
    orders = repository.list_orders(session)
    branches = [BranchOption.model_validate(b) for b in repository.list_enabled_branches(session)]
    customers = [CustomerOption.model_validate(c) for c in repository.list_active_customers(session)]
    agents = [AgentOption.model_validate(a) for a in repository.list_active_agents(session)]
    units = [InventoryOption.model_validate(u) for u in repository.list_available_inventory(session)]
    return templates.TemplateResponse(
        "orders.html",
        {
            "request": request,
            "orders": orders,
            "branches": branches,
            "customers": customers,
            "agents": agents,
            "units": units,
            "active_nav": "orders",
        },
    )


@router.get("/orders/options")
def order_options(session: Session = Depends(get_session)) -> dict:
    """Solo lectura, en vivo (no cacheado) - usado por el simulador de
    Order para elegir un par customer+inventory+agent real EN ESE
    MOMENTO, no con una lista tomada al cargar la pagina (mismo criterio
    que /ventas/opciones en Andes)."""
    return {
        "customers": [
            CustomerOption.model_validate(c).model_dump(mode="json")
            for c in repository.list_active_customers(session)
        ],
        "units": [
            InventoryOption.model_validate(u).model_dump(mode="json")
            for u in repository.list_available_inventory(session)
        ],
        "agents": [
            AgentOption.model_validate(a).model_dump(mode="json")
            for a in repository.list_active_agents(session)
        ],
    }


@router.post("/orders")
def create_order(
    buyer_code: str = Form(...),
    sales_agent_code: str = Form(...),
    fulfillment_branch: str = Form(...),
    chassis_number: str = Form(...),
    unit_gross_amount: str | None = Form(None),
    discount_pct: str = Form("0"),
    tax_amount: str = Form("0"),
    currency_code: str = Form("USD"),
    payment_channel: str | None = Form(None),
    captured_amount: str | None = Form(None),
    payment_reference: str | None = Form(None),
    session: Session = Depends(get_session),
) -> MutationResponse:
    correlation_id = str(uuid4())
    payload = _build_payload(
        buyer_code, sales_agent_code, fulfillment_branch, chassis_number, unit_gross_amount,
        discount_pct, tax_amount, currency_code, payment_channel, captured_amount, payment_reference,
    )
    try:
        order, events = service.create_order(session, payload, correlation_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ValidationDomainError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    return _response(correlation_id, events, order)


@router.post("/orders/{order_number}/edit")
def edit_order(
    order_number: str,
    order_status: str = Form(...),
    invoice_reference: str | None = Form(None),
    session: Session = Depends(get_session),
) -> MutationResponse:
    correlation_id = str(uuid4())
    try:
        payload = OrderEdicion(order_status=order_status, invoice_reference=invoice_reference or None)  # type: ignore[arg-type]
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=jsonable_encoder(exc.errors()))

    try:
        order, events = service.edit_order(session, order_number, payload, correlation_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    return _response(correlation_id, events, order)


@router.post("/orders/{order_number}/delete")
def delete_order(order_number: str, session: Session = Depends(get_session)) -> MutationResponse:
    correlation_id = str(uuid4())
    try:
        events = service.delete_order(session, order_number, correlation_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    return _response(correlation_id, events, None)

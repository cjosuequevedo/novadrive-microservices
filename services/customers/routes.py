"""Rutas de Customer - alta, edicion, eliminacion (seccion 8: POST
/customers, POST /customers/{code}/edit, POST /customers/{code}/delete)."""

from uuid import uuid4

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse
from pydantic import ValidationError
from sqlalchemy.orm import Session

from common.db import get_session
from common.errors import ConflictError, NotFoundError
from common.schemas.common import EventRef, MutationResponse
from common.security import require_auth
from services.customers import repository, service
from services.customers.schemas import CustomerEntrada, CustomerOut
from services.customers.web import templates

router = APIRouter(dependencies=[Depends(require_auth)])


def _build_payload(
    document_number: str,
    full_name: str,
    email_address: str | None,
    mobile_phone: str | None,
    mailing_address: str | None,
    birth_date: str | None,
    customer_status: str,
) -> CustomerEntrada:
    try:
        return CustomerEntrada(
            document_number=document_number,
            full_name=full_name,
            email_address=email_address or None,
            mobile_phone=mobile_phone or None,
            mailing_address=mailing_address or None,
            birth_date=birth_date or None,  # type: ignore[arg-type]
            customer_status=customer_status,  # type: ignore[arg-type]
        )
    except ValidationError as exc:
        # jsonable_encoder, no exc.errors() a secas - ver hallazgo #13 de
        # Andes (un Decimal/date invalido en el detalle revienta el
        # JSONResponse por defecto con un 500 en vez del 422 esperado).
        raise HTTPException(status_code=422, detail=jsonable_encoder(exc.errors()))


def _response(correlation_id: str, events, customer) -> MutationResponse:
    return MutationResponse(
        correlation_id=correlation_id,
        events=[EventRef(event_id=e.event_id, entity=e.entity_name, operation=e.operation) for e in events],
        data=CustomerOut.model_validate(customer).model_dump(mode="json") if customer is not None else None,
    )


@router.get("/customers", response_class=HTMLResponse)
def customers_page(request: Request, session: Session = Depends(get_session)):
    customers = repository.list_customers(session)
    return templates.TemplateResponse(
        "customers.html", {"request": request, "customers": customers, "active_nav": "customers"}
    )


@router.post("/customers")
def create_customer(
    document_number: str = Form(...),
    full_name: str = Form(...),
    email_address: str | None = Form(None),
    mobile_phone: str | None = Form(None),
    mailing_address: str | None = Form(None),
    birth_date: str | None = Form(None),
    customer_status: str = Form("A"),
    session: Session = Depends(get_session),
) -> MutationResponse:
    correlation_id = str(uuid4())
    payload = _build_payload(
        document_number, full_name, email_address, mobile_phone, mailing_address, birth_date, customer_status
    )
    try:
        customer, events = service.create_customer(session, payload, correlation_id)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    return _response(correlation_id, events, customer)


@router.post("/customers/{customer_code}/edit")
def edit_customer(
    customer_code: str,
    document_number: str = Form(...),
    full_name: str = Form(...),
    email_address: str | None = Form(None),
    mobile_phone: str | None = Form(None),
    mailing_address: str | None = Form(None),
    birth_date: str | None = Form(None),
    customer_status: str = Form("A"),
    session: Session = Depends(get_session),
) -> MutationResponse:
    correlation_id = str(uuid4())
    payload = _build_payload(
        document_number, full_name, email_address, mobile_phone, mailing_address, birth_date, customer_status
    )
    try:
        customer, events = service.edit_customer(session, customer_code, payload, correlation_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    return _response(correlation_id, events, customer)


@router.post("/customers/{customer_code}/delete")
def delete_customer(customer_code: str, session: Session = Depends(get_session)) -> MutationResponse:
    correlation_id = str(uuid4())
    try:
        events = service.delete_customer(session, customer_code, correlation_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    return _response(correlation_id, events, None)

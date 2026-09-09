"""
service.py - Reglas transaccionales de negocio para Customer (seccion 9
del documento).

Cada funcion publica:
1. Usa la transaccion de `session` (ya abierta por el caller via
   Depends(get_session)).
2. Valida las reglas de negocio (documento duplicado, no eliminar con
   ordenes).
3. Aplica el cambio via SQLAlchemy.
4. Construye el evento outbox con before/after y lo encola (sin commit).
5. Ejecuta UN SOLO commit (negocio + outbox juntos). Si algo falla
   antes de este punto, no se ha hecho ningun commit.
6. Devuelve el registro actualizado y los eventos generados, para que
   la ruta arme la respuesta con correlation_id + event_id (seccion 8).
"""

import secrets
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from common.events.builder import build_event
from common.models.customer import Customer
from common.models.outbox_event import OutboxEvent
from common.outbox.publisher import enqueue_outbox_event
from common.errors import ConflictError, NotFoundError
from services.customers import repository
from services.customers.schemas import CustomerEntrada


def _generate_customer_code() -> str:
    """CUS- + 10 caracteres hex en mayuscula (decision de diseno #5 en
    CLAUDE.md) - cabe en VARCHAR(16) (4 + 10 = 14). Generado con
    `secrets`, nunca inventado a mano ni derivado de datos predecibles."""
    return f"CUS-{secrets.token_hex(5).upper()}"


def _snapshot(customer: Customer) -> dict[str, Any]:
    """Representa el estado actual del customer como dict
    JSON-serializable, con las claves del contrato canonico ND_CUSTOMER
    (para que quien consuma Bronze despues las reconozca tal cual)."""
    return {
        "CUSTOMER_CODE": customer.customer_code,
        "DOCUMENT_NUMBER": customer.document_number,
        "FULL_NAME": customer.full_name,
        "EMAIL_ADDRESS": customer.email_address,
        "MOBILE_PHONE": customer.mobile_phone,
        "MAILING_ADDRESS": customer.mailing_address,
        "BIRTH_DATE": customer.birth_date,
        "CUSTOMER_STATUS": customer.customer_status,
    }


def create_customer(
    session: Session, payload: CustomerEntrada, correlation_id: str
) -> tuple[Customer, list[OutboxEvent]]:
    if repository.document_exists(session, payload.document_number):
        raise ConflictError(f"A customer with document {payload.document_number} already exists")

    customer_code = _generate_customer_code()
    # Colision de codigo generado: astronomicamente improbable (2**40
    # combinaciones), pero se verifica igual antes de confiar en el
    # INSERT - no cuesta nada y evita un IntegrityError confuso.
    while repository.code_exists(session, customer_code):
        customer_code = _generate_customer_code()

    customer = Customer(
        customer_code=customer_code,
        document_number=payload.document_number,
        full_name=payload.full_name,
        email_address=payload.email_address,
        mobile_phone=payload.mobile_phone,
        mailing_address=payload.mailing_address,
        birth_date=payload.birth_date,
        customer_status=payload.customer_status,
    )
    session.add(customer)
    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        raise ConflictError("Duplicate document_number (uq_customer_document constraint)") from exc

    after = _snapshot(customer)
    event = build_event(
        entity="customer",
        operation="c",
        record_key={"CUSTOMER_CODE": customer.customer_code},
        before=None,
        after=after,
        correlation_id=correlation_id,
    )
    outbox_row = enqueue_outbox_event(session, event, record_key_value=customer.customer_code)

    session.commit()  # ONE commit: business row + outbox row together
    session.refresh(customer)
    return customer, [outbox_row]


def edit_customer(
    session: Session, customer_code: str, payload: CustomerEntrada, correlation_id: str
) -> tuple[Customer, list[OutboxEvent]]:
    customer = repository.get_for_update(session, customer_code)
    if customer is None:
        raise NotFoundError(f"Customer {customer_code} does not exist")

    before = _snapshot(customer)

    if repository.document_exists(session, payload.document_number, exclude_customer_code=customer_code):
        raise ConflictError(f"Another customer already has document {payload.document_number}")

    customer.document_number = payload.document_number
    customer.full_name = payload.full_name
    customer.email_address = payload.email_address
    customer.mobile_phone = payload.mobile_phone
    customer.mailing_address = payload.mailing_address
    customer.birth_date = payload.birth_date
    customer.customer_status = payload.customer_status

    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        raise ConflictError("Duplicate document_number (uq_customer_document constraint)") from exc

    after = _snapshot(customer)
    event = build_event(
        entity="customer",
        operation="u",
        record_key={"CUSTOMER_CODE": customer_code},
        before=before,
        after=after,
        correlation_id=correlation_id,
    )
    outbox_row = enqueue_outbox_event(session, event, record_key_value=customer_code)

    session.commit()
    session.refresh(customer)
    return customer, [outbox_row]


def delete_customer(session: Session, customer_code: str, correlation_id: str) -> list[OutboxEvent]:
    customer = repository.get_for_update(session, customer_code)
    if customer is None:
        raise NotFoundError(f"Customer {customer_code} does not exist")

    # Business rule (section 9): "No eliminar si ya tiene ordenes."
    if repository.has_orders(session, customer_code):
        raise ConflictError(f"Customer {customer_code} has orders on record; cannot be deleted")

    before = _snapshot(customer)
    session.delete(customer)
    session.flush()

    event = build_event(
        entity="customer",
        operation="d",
        record_key={"CUSTOMER_CODE": customer_code},
        before=before,
        after=None,
        correlation_id=correlation_id,
    )
    outbox_row = enqueue_outbox_event(session, event, record_key_value=customer_code)

    session.commit()
    return [outbox_row]

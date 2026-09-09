"""
service.py - Reglas transaccionales de negocio para Order (seccion 9
del documento). La mas compleja de las tres: cabecera + item + pago
opcional + `FOR UPDATE` sobre el inventory unit + DOS eventos (sale +
vehicle) con el mismo correlation_id, todo en un solo commit - mismo
patron que Andes con Venta, adaptado a las tablas/nombres NovaDrive y a
descuento-como-porcentaje en vez de monto.
"""

import secrets
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from sqlalchemy.orm import Session

from common.events.builder import build_event
from common.models.inventory_unit import InventoryUnit
from common.models.order_item import OrderItem
from common.models.outbox_event import OutboxEvent
from common.models.payment_txn import PaymentTxn
from common.models.sales_order import SalesOrder
from common.outbox.publisher import enqueue_outbox_event
from common.errors import ConflictError, NotFoundError, ValidationDomainError
from services.orders import repository
from services.orders.schemas import OrderEdicion, OrderEntrada

FINAL_STATES = {"INVOICED", "CANCELLED"}
VALID_TRANSITIONS: dict[str, set[str]] = {
    "OPEN": {"APPROVED", "CANCELLED"},
    "APPROVED": {"INVOICED", "CANCELLED"},
    "INVOICED": set(),
    "CANCELLED": set(),
}

_TWO_PLACES = Decimal("0.01")


def _round2(value: Decimal) -> Decimal:
    return value.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)


def _generate_order_number() -> str:
    """ORD-YYYYMMDD-HHMMSS-XXXX (decision de diseno #5 en CLAUDE.md,
    mismo espiritu que el numero de factura autogenerado de Andes).
    4+8+1+6+1+4 = 24 caracteres, cabe exacto en VARCHAR(24)."""
    now = datetime.now(timezone.utc)
    return f"ORD-{now.strftime('%Y%m%d')}-{now.strftime('%H%M%S')}-{secrets.token_hex(2).upper()}"


def _snapshot_order(order: SalesOrder, chassis_number: str | None) -> dict[str, Any]:
    return {
        "ORDER_NUMBER": order.order_number,
        "BUYER_CODE": order.buyer_code,
        "SALES_AGENT_CODE": order.sales_agent_code,
        "FULFILLMENT_BRANCH": order.fulfillment_branch,
        "ORDERED_AT": order.ordered_at,
        "GROSS_AMOUNT": order.gross_amount,
        "DISCOUNT_PCT": order.discount_pct,
        "TAX_AMOUNT": order.tax_amount,
        "NET_AMOUNT": order.net_amount,
        "CURRENCY_CODE": order.currency_code,
        "ORDER_STATUS": order.order_status,
        "INVOICE_REFERENCE": order.invoice_reference,
        "CHASSIS_NUMBER": chassis_number,  # seccion 17: "Incorporar desde el agregado sale"
    }


def _snapshot_unit(unit: InventoryUnit) -> dict[str, Any]:
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


def create_order(
    session: Session, payload: OrderEntrada, correlation_id: str
) -> tuple[SalesOrder, list[OutboxEvent]]:
    customer = repository.get_active_customer(session, payload.buyer_code)
    if customer is None:
        raise ValidationDomainError(f"customer {payload.buyer_code} does not exist or is not active")

    agent = repository.get_active_agent_in_branch(session, payload.sales_agent_code, payload.fulfillment_branch)
    if agent is None:
        raise ValidationDomainError(
            f"sales agent {payload.sales_agent_code} does not exist, is not active, "
            f"or does not belong to branch {payload.fulfillment_branch}"
        )

    unit = repository.get_inventory_unit_for_update(session, payload.chassis_number)  # FOR UPDATE
    if unit is None:
        raise NotFoundError(f"inventory unit {payload.chassis_number} does not exist")
    if unit.branch_code != payload.fulfillment_branch:
        raise ValidationDomainError("inventory unit does not belong to the fulfillment branch")
    if unit.availability_code != "AVL":
        # Cubre doble venta: la segunda transaccion encuentra la unidad ya
        # no AVL (bloqueada por FOR UPDATE hasta que la primera termine) y
        # falla sin cambio parcial.
        raise ConflictError(f"inventory unit {unit.chassis_number} is not AVL (availability={unit.availability_code})")

    unit_gross_amount = payload.unit_gross_amount if payload.unit_gross_amount is not None else unit.list_amount
    gross_amount = unit_gross_amount
    net_amount = _round2(gross_amount * (Decimal(1) - payload.discount_pct / Decimal(100))) + payload.tax_amount
    unit_net_amount = _round2(unit_gross_amount * (Decimal(1) - payload.discount_pct / Decimal(100)))

    order_number = _generate_order_number()
    while repository.order_number_exists(session, order_number):
        order_number = _generate_order_number()

    order = SalesOrder(
        order_number=order_number,
        buyer_code=payload.buyer_code,
        sales_agent_code=payload.sales_agent_code,
        fulfillment_branch=payload.fulfillment_branch,
        gross_amount=gross_amount,
        discount_pct=payload.discount_pct,
        tax_amount=payload.tax_amount,
        net_amount=net_amount,
        currency_code=payload.currency_code,
        order_status="OPEN",
    )
    session.add(order)
    session.flush()

    item = OrderItem(
        order_number=order.order_number,
        line_number=1,
        chassis_number=unit.chassis_number,
        unit_gross_amount=unit_gross_amount,
        line_discount_pct=payload.discount_pct,
        unit_net_amount=unit_net_amount,
    )
    session.add(item)

    if payload.payment_channel and payload.captured_amount is not None:
        session.add(
            PaymentTxn(
                payment_reference=payload.payment_reference or f"PAY-{secrets.token_hex(6).upper()}",
                order_number=order.order_number,
                payment_channel=payload.payment_channel,
                captured_amount=payload.captured_amount,
                currency_code=payload.currency_code,
            )
        )

    unit_before = _snapshot_unit(unit)
    unit.availability_code = "SOLD"
    session.flush()
    session.refresh(order)
    session.refresh(unit)
    unit_after = _snapshot_unit(unit)
    order_after = _snapshot_order(order, unit.chassis_number)

    events: list[OutboxEvent] = []
    ev_sale = build_event(
        entity="sale",
        operation="c",
        record_key={"ORDER_NUMBER": order.order_number},
        before=None,
        after=order_after,
        correlation_id=correlation_id,
    )
    events.append(enqueue_outbox_event(session, ev_sale, record_key_value=order.order_number))

    ev_vehicle = build_event(
        entity="vehicle",
        operation="u",
        record_key={"CHASSIS_NUMBER": unit.chassis_number},
        before=unit_before,
        after=unit_after,
        correlation_id=correlation_id,
    )
    events.append(enqueue_outbox_event(session, ev_vehicle, record_key_value=unit.chassis_number))

    session.commit()  # ONE commit: order + item + payment + inventory update + both outbox rows
    session.refresh(order)
    return order, events


def edit_order(
    session: Session, order_number: str, payload: OrderEdicion, correlation_id: str
) -> tuple[SalesOrder, list[OutboxEvent]]:
    order = repository.get_for_update(session, order_number)
    if order is None:
        raise NotFoundError(f"order {order_number} does not exist")

    item = repository.get_item(session, order_number)
    chassis_number = item.chassis_number if item else None
    before = _snapshot_order(order, chassis_number)

    if order.order_status in FINAL_STATES:
        raise ConflictError(f"order {order_number} is already in a final state ({order.order_status})")
    if payload.order_status != order.order_status and payload.order_status not in VALID_TRANSITIONS.get(
        order.order_status, set()
    ):
        raise ConflictError(f"invalid transition from {order.order_status} to {payload.order_status}")

    order.order_status = payload.order_status
    if payload.invoice_reference is not None:
        order.invoice_reference = payload.invoice_reference

    session.flush()
    session.refresh(order)
    after = _snapshot_order(order, chassis_number)

    event = build_event(
        entity="sale",
        operation="u",
        record_key={"ORDER_NUMBER": order_number},
        before=before,
        after=after,
        correlation_id=correlation_id,
    )
    outbox_row = enqueue_outbox_event(session, event, record_key_value=order_number)

    session.commit()
    session.refresh(order)
    return order, [outbox_row]


def delete_order(session: Session, order_number: str, correlation_id: str) -> list[OutboxEvent]:
    order = repository.get_for_update(session, order_number)
    if order is None:
        raise NotFoundError(f"order {order_number} does not exist")

    # Section 9, "Eliminacion de venta": solo estados reversibles.
    if order.order_status not in ("OPEN", "APPROVED"):
        raise ConflictError(
            f"order {order_number} is in state {order.order_status}; can only be deleted from OPEN or APPROVED"
        )

    payments = repository.get_payments(session, order_number)
    if any(p.captured_at is not None for p in payments):
        raise ConflictError(f"order {order_number} has a captured payment; cannot be deleted")

    item = repository.get_item(session, order_number)
    unit = repository.get_inventory_unit_for_update(session, item.chassis_number) if item else None

    before_order = _snapshot_order(order, item.chassis_number if item else None)
    unit_before = _snapshot_unit(unit) if unit else None

    for p in payments:
        session.delete(p)
    if item:
        session.delete(item)
    session.delete(order)

    if unit is not None:
        unit.availability_code = "AVL"  # release the inventory unit

    session.flush()

    events: list[OutboxEvent] = []
    ev_sale = build_event(
        entity="sale",
        operation="d",
        record_key={"ORDER_NUMBER": order_number},
        before=before_order,
        after=None,
        correlation_id=correlation_id,
    )
    events.append(enqueue_outbox_event(session, ev_sale, record_key_value=order_number))

    if unit is not None:
        session.refresh(unit)
        unit_after = _snapshot_unit(unit)
        ev_vehicle = build_event(
            entity="vehicle",
            operation="u",
            record_key={"CHASSIS_NUMBER": unit.chassis_number},
            before=unit_before,
            after=unit_after,
            correlation_id=correlation_id,
        )
        events.append(enqueue_outbox_event(session, ev_vehicle, record_key_value=unit.chassis_number))

    session.commit()
    return events

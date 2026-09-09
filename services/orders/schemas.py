"""Schemas Pydantic de Order - request/response del formulario.

Una orden en este MVP tiene exactamente una linea (un `ND_ORDER_ITEM`
por `ND_SALES_ORDER`) - mismo criterio de simplificacion que Andes usa
para Venta (un `AM_VENTA_DETALLE` por `AM_VENTA`). `order_number` no lo
escribe el usuario (se autogenera, ver service.py), y la edicion solo
expone estado + referencia de factura (ver razonamiento en CLAUDE.md,
estandar de interfaz punto 4)."""

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from common.schemas.validators import DiscountPercentage, NonNegativeAmount

OrderStatus = Literal["OPEN", "APPROVED", "INVOICED", "CANCELLED"]


class OrderEntrada(BaseModel):
    buyer_code: str = Field(min_length=1, max_length=16)
    sales_agent_code: str = Field(min_length=1, max_length=12)
    fulfillment_branch: str = Field(min_length=1, max_length=8)
    chassis_number: str = Field(min_length=17, max_length=17)
    # Si no se especifica, el servicio usa inventory_unit.list_amount -
    # mismo criterio que Andes con precio_unitario/precio_lista.
    unit_gross_amount: NonNegativeAmount | None = None
    discount_pct: DiscountPercentage = Decimal("0")
    tax_amount: NonNegativeAmount = Decimal("0")
    currency_code: str = Field(default="USD", min_length=3, max_length=3)
    payment_channel: str | None = None
    captured_amount: NonNegativeAmount | None = None
    payment_reference: str | None = None


class OrderEdicion(BaseModel):
    """Solo estado + referencia de factura - reasignar buyer/agent/
    inventory despues de creada la orden rompería la disponibilidad ya
    comprometida del inventory unit (mismo razonamiento que Andes con
    Venta)."""

    order_status: OrderStatus
    invoice_reference: str | None = None


class OrderOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    order_number: str
    buyer_code: str
    sales_agent_code: str
    fulfillment_branch: str
    gross_amount: Decimal
    discount_pct: Decimal
    tax_amount: Decimal
    net_amount: Decimal
    currency_code: str
    order_status: str
    invoice_reference: str | None


class CustomerOption(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    customer_code: str
    full_name: str


class AgentOption(BaseModel):
    model_config = ConfigDict(from_attributes=True, protected_namespaces=())

    agent_code: str
    agent_display_name: str
    branch_code: str


class InventoryOption(BaseModel):
    model_config = ConfigDict(from_attributes=True, protected_namespaces=())

    chassis_number: str
    brand_name: str
    model_name: str
    branch_code: str
    list_amount: Decimal


class BranchOption(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    branch_code: str
    display_name: str

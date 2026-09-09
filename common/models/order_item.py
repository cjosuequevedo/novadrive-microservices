"""ND_ORDER_ITEM -> nd.order_item - lineas vehiculo<->orden.

Sin formulario ni topico propio (va dentro del evento `sale`, ver
CLAUDE.md seccion "Contrato de eventos"). PK compuesta
(order_number, line_number), igual que el PDF la define.
"""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import ForeignKey, Numeric, SmallInteger, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from common.models.base import Base


class OrderItem(Base):
    __tablename__ = "order_item"

    order_number: Mapped[str] = mapped_column(
        String(24), ForeignKey("nd.sales_order.order_number"), primary_key=True
    )
    line_number: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    chassis_number: Mapped[str] = mapped_column(
        String(17), ForeignKey("nd.inventory_unit.chassis_number"), nullable=False, unique=True
    )
    unit_gross_amount: Mapped[Decimal] = mapped_column(Numeric(16, 2), nullable=False)
    line_discount_pct: Mapped[Decimal] = mapped_column(Numeric(7, 4), nullable=False, server_default="0")
    unit_net_amount: Mapped[Decimal] = mapped_column(Numeric(16, 2), nullable=False)
    created_on: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())
    last_changed_on: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())

    sales_order: Mapped["SalesOrder"] = relationship(back_populates="items")  # noqa: F821
    inventory_unit: Mapped["InventoryUnit"] = relationship(back_populates="order_item")  # noqa: F821

    def __repr__(self) -> str:
        return f"OrderItem(order={self.order_number!r}, line={self.line_number!r}, chassis={self.chassis_number!r})"

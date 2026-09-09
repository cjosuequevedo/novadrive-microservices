"""ND_SALES_ORDER -> nd.sales_order - cabecera comercial de una orden."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import CHAR, ForeignKey, Numeric, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from common.models.base import Base


class SalesOrder(Base):
    __tablename__ = "sales_order"

    order_number: Mapped[str] = mapped_column(String(24), primary_key=True)
    buyer_code: Mapped[str] = mapped_column(String(16), ForeignKey("nd.customer.customer_code"), nullable=False)
    sales_agent_code: Mapped[str] = mapped_column(
        String(12), ForeignKey("nd.sales_agent.agent_code"), nullable=False
    )
    fulfillment_branch: Mapped[str] = mapped_column(
        String(8), ForeignKey("nd.branch.branch_code"), nullable=False
    )
    ordered_at: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())
    gross_amount: Mapped[Decimal] = mapped_column(Numeric(16, 2), nullable=False)
    discount_pct: Mapped[Decimal] = mapped_column(Numeric(7, 4), nullable=False, server_default="0")
    tax_amount: Mapped[Decimal] = mapped_column(Numeric(16, 2), nullable=False, server_default="0")
    net_amount: Mapped[Decimal] = mapped_column(Numeric(16, 2), nullable=False)
    currency_code: Mapped[str] = mapped_column(CHAR(3), nullable=False, server_default="USD")
    order_status: Mapped[str] = mapped_column(String(10), nullable=False, server_default="OPEN")
    invoice_reference: Mapped[str | None] = mapped_column(String(35))
    created_on: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())
    last_changed_on: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())

    customer: Mapped["Customer"] = relationship(back_populates="sales_orders")  # noqa: F821
    sales_agent: Mapped["SalesAgent"] = relationship(back_populates="sales_orders")  # noqa: F821
    branch: Mapped["Branch"] = relationship(back_populates="sales_orders")  # noqa: F821
    items: Mapped[list["OrderItem"]] = relationship(back_populates="sales_order")  # noqa: F821
    payments: Mapped[list["PaymentTxn"]] = relationship(back_populates="sales_order")  # noqa: F821

    def __repr__(self) -> str:
        return f"SalesOrder(number={self.order_number!r}, status={self.order_status!r})"

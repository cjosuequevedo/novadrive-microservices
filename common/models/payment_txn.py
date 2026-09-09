"""ND_PAYMENT_TXN -> nd.payment_txn - transacciones de cobro.

Sin topico propio en el MVP (va dentro del evento `sale`).
"""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import CHAR, ForeignKey, Numeric, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from common.models.base import Base


class PaymentTxn(Base):
    __tablename__ = "payment_txn"

    payment_reference: Mapped[str] = mapped_column(String(30), primary_key=True)
    order_number: Mapped[str] = mapped_column(
        String(24), ForeignKey("nd.sales_order.order_number"), nullable=False
    )
    payment_channel: Mapped[str] = mapped_column(String(12), nullable=False)
    captured_amount: Mapped[Decimal] = mapped_column(Numeric(16, 2), nullable=False)
    currency_code: Mapped[str] = mapped_column(CHAR(3), nullable=False, server_default="USD")
    payment_status: Mapped[str] = mapped_column(String(10), nullable=False, server_default="PENDING")
    captured_at: Mapped[datetime | None] = mapped_column()
    provider_code: Mapped[str | None] = mapped_column(String(30))
    created_on: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())
    last_changed_on: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())

    sales_order: Mapped["SalesOrder"] = relationship(back_populates="payments")  # noqa: F821

    def __repr__(self) -> str:
        return f"PaymentTxn(ref={self.payment_reference!r}, order={self.order_number!r})"

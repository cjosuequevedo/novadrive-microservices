"""ND_CUSTOMER -> nd.customer - personas o empresas compradoras."""

from datetime import date, datetime

from sqlalchemy import CHAR, Date, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from common.models.base import Base


class Customer(Base):
    __tablename__ = "customer"

    customer_code: Mapped[str] = mapped_column(String(16), primary_key=True)
    document_number: Mapped[str] = mapped_column(String(25), nullable=False, unique=True)
    full_name: Mapped[str] = mapped_column(String(180), nullable=False)
    email_address: Mapped[str | None] = mapped_column(String(180))
    mobile_phone: Mapped[str | None] = mapped_column(String(35))
    mailing_address: Mapped[str | None] = mapped_column(String(240))
    birth_date: Mapped[date | None] = mapped_column(Date)
    customer_status: Mapped[str] = mapped_column(CHAR(1), nullable=False, server_default="A")
    created_on: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())
    last_changed_on: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())

    sales_orders: Mapped[list["SalesOrder"]] = relationship(back_populates="customer")  # noqa: F821

    def __repr__(self) -> str:
        return f"Customer(code={self.customer_code!r}, doc={self.document_number!r})"

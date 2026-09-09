"""ND_INVENTORY_UNIT -> nd.inventory_unit - inventario unitario por chasis."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import ForeignKey, Numeric, SmallInteger, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from common.models.base import Base


class InventoryUnit(Base):
    __tablename__ = "inventory_unit"

    chassis_number: Mapped[str] = mapped_column(String(17), primary_key=True)
    branch_code: Mapped[str] = mapped_column(String(8), ForeignKey("nd.branch.branch_code"), nullable=False)
    brand_name: Mapped[str] = mapped_column(String(60), nullable=False)
    model_name: Mapped[str] = mapped_column(String(100), nullable=False)
    model_year: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    exterior_colour: Mapped[str | None] = mapped_column(String(50))
    list_amount: Mapped[Decimal] = mapped_column(Numeric(15, 2), nullable=False)
    availability_code: Mapped[str] = mapped_column(String(10), nullable=False, server_default="AVL")
    received_at: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())
    created_on: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())
    last_changed_on: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())

    branch: Mapped["Branch"] = relationship(back_populates="inventory_units")  # noqa: F821
    order_item: Mapped["OrderItem | None"] = relationship(back_populates="inventory_unit")  # noqa: F821

    def __repr__(self) -> str:
        return f"InventoryUnit(chassis={self.chassis_number!r}, availability={self.availability_code!r})"

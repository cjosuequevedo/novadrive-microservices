"""ND_BRANCH -> nd.branch - catalogo de sedes (dato de referencia)."""

from datetime import datetime

from sqlalchemy import CHAR, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from common.models.base import Base


class Branch(Base):
    __tablename__ = "branch"

    branch_code: Mapped[str] = mapped_column(String(8), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(120), nullable=False)
    city_name: Mapped[str] = mapped_column(String(80), nullable=False)
    street_address: Mapped[str | None] = mapped_column(String(240))
    enabled_flag: Mapped[str] = mapped_column(CHAR(1), nullable=False, server_default="Y")
    created_on: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())
    last_changed_on: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())

    sales_agents: Mapped[list["SalesAgent"]] = relationship(back_populates="branch")  # noqa: F821
    inventory_units: Mapped[list["InventoryUnit"]] = relationship(back_populates="branch")  # noqa: F821
    sales_orders: Mapped[list["SalesOrder"]] = relationship(back_populates="branch")  # noqa: F821

    def __repr__(self) -> str:
        return f"Branch(code={self.branch_code!r}, name={self.display_name!r})"

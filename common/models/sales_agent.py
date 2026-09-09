"""ND_SALES_AGENT -> nd.sales_agent - asesores comerciales de una sede."""

from datetime import date, datetime

from sqlalchemy import Date, ForeignKey, SmallInteger, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from common.models.base import Base


class SalesAgent(Base):
    __tablename__ = "sales_agent"

    agent_code: Mapped[str] = mapped_column(String(12), primary_key=True)
    branch_code: Mapped[str] = mapped_column(String(8), ForeignKey("nd.branch.branch_code"), nullable=False)
    agent_display_name: Mapped[str] = mapped_column(String(160), nullable=False)
    corporate_email: Mapped[str | None] = mapped_column(String(180))
    active_flag: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default="1")
    hired_on: Mapped[date | None] = mapped_column(Date)
    created_on: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())
    last_changed_on: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())

    branch: Mapped["Branch"] = relationship(back_populates="sales_agents")  # noqa: F821
    sales_orders: Mapped[list["SalesOrder"]] = relationship(back_populates="sales_agent")  # noqa: F821

    def __repr__(self) -> str:
        return f"SalesAgent(code={self.agent_code!r}, branch={self.branch_code!r})"

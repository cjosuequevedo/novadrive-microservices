"""ND_OUTBOX_EVENT -> nd.outbox_event - cola tecnica durable (transactional outbox).

Sin FKs a proposito (ver DDL): el evento debe sobrevivir a la
eliminacion del registro de origen. No hay relationship() hacia otras
tablas por la misma razon - igual que en Andes.

A diferencia de las otras 7 tablas ND_*, estas columnas de fecha SI
llevan zona horaria (`TIMESTAMPTZ`) - ver decision de diseno #8 en
CLAUDE.md, que refleja una inconsistencia real del propio PDF entre
`ND_OUTBOX_EVENT` ("WITH TIME ZONE") y el resto de tablas
("LOCALTIMESTAMP", sin zona horaria).
"""

from datetime import datetime

from sqlalchemy import CHAR, BigInteger, DateTime, Identity, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from common.models.base import Base


class OutboxEvent(Base):
    __tablename__ = "outbox_event"

    outbox_id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    event_version: Mapped[int] = mapped_column(nullable=False, server_default="1")
    source_system: Mapped[str] = mapped_column(String(20), nullable=False, server_default="NOVADRIVE")
    entity_name: Mapped[str] = mapped_column(String(30), nullable=False)
    source_table: Mapped[str] = mapped_column(String(40), nullable=False)
    operation: Mapped[str] = mapped_column(CHAR(1), nullable=False)
    record_key_json: Mapped[str] = mapped_column(Text, nullable=False)
    kafka_topic: Mapped[str] = mapped_column(String(180), nullable=False)
    kafka_key: Mapped[str] = mapped_column(String(300), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(15), nullable=False, server_default="PENDING")
    attempts: Mapped[int] = mapped_column(nullable=False, server_default="0")
    next_retry_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(String(1000))
    correlation_id: Mapped[str] = mapped_column(String(36), nullable=False)

    def __repr__(self) -> str:
        return (
            f"OutboxEvent(event_id={self.event_id!r}, entity={self.entity_name!r}, "
            f"op={self.operation!r}, status={self.status!r})"
        )

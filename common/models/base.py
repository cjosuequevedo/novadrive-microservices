"""
base.py - Declarative base compartida por los 8 modelos ND_*.

Importante, mismo principio que Andes: el esquema real ya existe en
Postgres, creado y verificado a mano desde db/ddl_postgres.sql (ver
CLAUDE.md, Fase 1 - checkpoint verificado con information_schema y
constraints reales). Estos modelos MAPEAN ese esquema, no lo generan -
no se llama Base.metadata.create_all() contra la base real en ningun
punto del proyecto.

`MetaData(schema="nd")` fija el schema por defecto para las 8 tablas,
asi que cada modelo solo declara __tablename__ (minuscula, sin
prefijo) en vez de repetir `__table_args__ = {"schema": "nd"}` ocho
veces.
"""

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    metadata = MetaData(schema="nd")

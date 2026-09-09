"""
db.py - Engine y sesion SQLAlchemy 2 sobre Postgres via psycopg (driver
psycopg3, no psycopg2 - es el driver "python oracledb"-equivalente que
pide el stack obligatorio de CLAUDE.md para este proyecto).

A diferencia de Andes/Oracle, acá no hay hallazgo de service_name vs SID
que resolver: la URL de conexion a Postgres es la forma estandar
"postgresql+psycopg://user:pass@host:port/db", sin ambiguedad de
dialecto. `options=-csearch_path=nd` fija el schema por defecto de la
conexion para que las queries sin prefijo (`SELECT * FROM customer`)
resuelvan contra `nd.customer` sin tener que calificar cada tabla -
los modelos en common/models/ igual declaran `schema="nd"` explicito
en su metadata (ver base.py) para no depender solo de search_path.
"""

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker

from common.config import get_settings

settings = get_settings()


def _build_engine():
    connect_url = URL.create(
        "postgresql+psycopg",
        username=settings.postgres_user,
        password=settings.postgres_password,
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_db,
    )
    return create_engine(
        connect_url,
        pool_pre_ping=True,
        connect_args={"options": f"-csearch_path={settings.postgres_schema}"},
    )


engine = _build_engine()

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def get_session():
    """Dependency-style generator para FastAPI (Depends(get_session)) en
    cualquiera de los tres microservicios."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

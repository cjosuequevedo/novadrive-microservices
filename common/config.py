"""
config.py - Configuracion via Pydantic Settings, compartida por los tres
microservicios (customers/inventory/orders), el worker y el bridge.

Grupos de variables adaptados de la seccion 19 del documento (que los
define para Oracle) a Postgres: ORACLE_* -> POSTGRES_* (sin wallet,
Postgres no lo necesita). Nada de secretos hardcodeados: todo viene de
variables de entorno o de un .env local en desarrollo (gitignored).
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Aplicacion ---
    app_env: str = "development"
    app_username: str = "admin"
    app_password: str = "changeme"
    source_system: str = "NOVADRIVE"

    # --- Postgres ---
    # Usuario de aplicacion (novadrive_app), nunca un rol superusuario -
    # ver restriccion no negociable en CLAUDE.md ("No utilizar un rol
    # superusuario/ADMIN de Postgres desde las aplicaciones").
    postgres_user: str = "novadrive_app"
    postgres_password: str = "changeme"
    postgres_db: str = "novadrive"
    postgres_host: str = "127.0.0.1"
    postgres_port: int = 5432
    postgres_schema: str = "nd"

    # --- Redpanda / Kafka (broker propio de NovaDrive, ver CLAUDE.md) ---
    kafka_bootstrap_servers: str = "127.0.0.1:29092"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_username: str | None = None
    kafka_sasl_password: str | None = None

    # --- Outbox ---
    outbox_poll_ms: int = 1000
    outbox_batch_size: int = 20
    outbox_max_retries: int = 8
    # Visibility timeout: una fila PROCESSING mas vieja que esto se
    # considera abandonada (worker muerto a mitad de ciclo) y vuelve a
    # ser reclamable. Ver hallazgo real en CLAUDE.md, seccion Worker.
    stale_processing_sec: int = 120

    # --- Navegacion entre microservicios (decision de diseno #4 en CLAUDE.md) ---
    # URLs absolutas, no rutas relativas: cada microservicio es un proceso
    # y un puerto/dominio distinto. En local, tres puertos separados; en
    # produccion, tres dominios publicos separados (ver CLAUDE.md, seccion
    # Despliegue).
    customers_url: str = "http://127.0.0.1:8001"
    inventory_url: str = "http://127.0.0.1:8002"
    orders_url: str = "http://127.0.0.1:8003"

    # --- Esquema ---
    # El DDL (db/ddl_postgres.sql) se aplica manualmente/via
    # docker-entrypoint-initdb.d, revisado en el checkpoint de Fase 1 -
    # nunca automaticamente desde la app (ver common/models/base.py).
    auto_create_schema: bool = False


@lru_cache
def get_settings() -> Settings:
    return Settings()

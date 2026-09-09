"""
topics.py - Mapa entidad -> topico Redpanda -> clave de particion.

Fuente literal: seccion 12 del documento ("Topicos Redpanda"), fila
NovaDrive. No crear topicos distintos para c/u/d (regla explicita de
la misma seccion) - las tres operaciones de una entidad comparten un
topico. `source_table` usa el nombre CANONICO en mayuscula del PDF
(ND_CUSTOMER, no nd.customer) - ver decision de diseno #9 en
CLAUDE.md: el campo va en el contrato de eventos, no es el nombre
fisico de la tabla en Postgres.
"""

from typing import Final, Literal

Entity = Literal["customer", "vehicle", "sale"]

# entity -> (source_table canonico ND_*, topico Redpanda, campo de la PK usado en la clave)
ENTITY_CONFIG: Final[dict[Entity, dict[str, str]]] = {
    "customer": {
        "source_table": "ND_CUSTOMER",
        "topic": "cdc.novadrive.customer",
        "key_field": "CUSTOMER_CODE",
    },
    "vehicle": {
        "source_table": "ND_INVENTORY_UNIT",
        "topic": "cdc.novadrive.vehicle",
        "key_field": "CHASSIS_NUMBER",
    },
    "sale": {
        "source_table": "ND_SALES_ORDER",
        "topic": "cdc.novadrive.sale",
        "key_field": "ORDER_NUMBER",
    },
}

SOURCE_SYSTEM: Final[str] = "NOVADRIVE"


def source_table_for(entity: Entity) -> str:
    return ENTITY_CONFIG[entity]["source_table"]


def topic_for(entity: Entity) -> str:
    return ENTITY_CONFIG[entity]["topic"]


def kafka_key_for(entity: Entity, record_key_value: object) -> str:
    """Clave estable de particion, ej. 'NOVADRIVE|customer|CUS0001'.

    Todos los eventos del mismo registro deben conservar la misma
    clave (seccion 11) - por eso se deriva siempre del mismo campo de
    PK/codigo de negocio, nunca de un valor que pueda cambiar entre
    eventos.
    """
    return f"{SOURCE_SYSTEM}|{entity}|{record_key_value}"

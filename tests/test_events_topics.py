"""Verifica el mapa entidad->topico->clave contra la seccion 12 literal
(fila NovaDrive)."""

from common.events.topics import kafka_key_for, source_table_for, topic_for


def test_topicos_exactos_seccion_12():
    assert topic_for("customer") == "cdc.novadrive.customer"
    assert topic_for("vehicle") == "cdc.novadrive.vehicle"
    assert topic_for("sale") == "cdc.novadrive.sale"


def test_source_table_por_entidad():
    assert source_table_for("customer") == "ND_CUSTOMER"
    assert source_table_for("vehicle") == "ND_INVENTORY_UNIT"
    assert source_table_for("sale") == "ND_SALES_ORDER"


def test_kafka_key_formato_exacto():
    assert kafka_key_for("customer", "CUS0001") == "NOVADRIVE|customer|CUS0001"
    assert kafka_key_for("vehicle", "1HGCM82633A004352") == "NOVADRIVE|vehicle|1HGCM82633A004352"
    assert kafka_key_for("sale", "ORD-0001") == "NOVADRIVE|sale|ORD-0001"


def test_kafka_key_misma_clave_para_mismo_registro():
    # "Todos los eventos del mismo registro deben conservar la misma clave"
    k1 = kafka_key_for("vehicle", "1HGCM82633A004352")
    k2 = kafka_key_for("vehicle", "1HGCM82633A004352")
    assert k1 == k2


def test_no_hay_topicos_distintos_para_c_u_d():
    # Regla explicita de la seccion 12: un unico topico por entidad,
    # independiente de si el evento es c, u o d - no existe, por ejemplo,
    # "cdc.novadrive.customer.created" ni variantes por operacion.
    for entity in ("customer", "vehicle", "sale"):
        topic = topic_for(entity)
        assert topic.count(".") == 2
        assert topic.startswith("cdc.novadrive.")

"""
test_bridge_grouping.py - group_by_volume() no depende de red ni de
Kafka/Databricks reales (mismo criterio que el resto de tests/: cubre
lo que no depende de infraestructura viva). Verifica el split de
Volume por tipo de evento (Fase 15, mismo patron replicado de Andes).
"""

from datetime import datetime, timezone

from bridge.bridge_redpanda_to_databricks import VOLUME, group_by_volume


def _msg(payload: str) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "kafka_topic": "cdc.novadrive.customer",
        "kafka_partition": 0,
        "kafka_offset": 1,
        "kafka_timestamp": now,
        "ingested_at": now,
        "payload": payload,
    }


def test_entidad_conocida_va_al_volume_especifico():
    m = _msg('{"entity": "customer", "event_id": "1"}')
    groups = group_by_volume([m])
    assert list(groups.keys()) == [f"{VOLUME}_customer"]
    assert groups[f"{VOLUME}_customer"] == [m]


def test_las_3_entidades_conocidas_van_cada_una_a_su_volume():
    msgs = [
        _msg('{"entity": "customer", "event_id": "1"}'),
        _msg('{"entity": "vehicle", "event_id": "2"}'),
        _msg('{"entity": "sale", "event_id": "3"}'),
    ]
    groups = group_by_volume(msgs)
    assert set(groups.keys()) == {f"{VOLUME}_customer", f"{VOLUME}_vehicle", f"{VOLUME}_sale"}
    for group in groups.values():
        assert len(group) == 1


def test_entity_desconocida_cae_al_volume_generico():
    m = _msg('{"entity": "algo_que_no_existe", "event_id": "1"}')
    groups = group_by_volume([m])
    assert list(groups.keys()) == [VOLUME]


def test_entity_faltante_cae_al_volume_generico():
    m = _msg('{"event_id": "1"}')  # sin campo "entity"
    groups = group_by_volume([m])
    assert list(groups.keys()) == [VOLUME]


def test_payload_no_es_json_valido_cae_al_volume_generico_sin_romper():
    m = _msg("esto no es json valido {{{")
    groups = group_by_volume([m])
    assert list(groups.keys()) == [VOLUME]


def test_payload_json_pero_no_es_un_objeto_cae_al_volume_generico():
    m = _msg("[1, 2, 3]")  # JSON valido, pero no un dict con "entity"
    groups = group_by_volume([m])
    assert list(groups.keys()) == [VOLUME]


def test_batch_mixto_produce_multiples_grupos_ningun_mensaje_se_pierde():
    msgs = [
        _msg('{"entity": "sale", "event_id": "1"}'),
        _msg('{"entity": "vehicle", "event_id": "2"}'),
        _msg('{"entity": "sale", "event_id": "3"}'),
        _msg("payload corrupto"),
    ]
    groups = group_by_volume(msgs)

    total_mensajes_agrupados = sum(len(g) for g in groups.values())
    assert total_mensajes_agrupados == len(msgs)
    assert set(groups.keys()) == {f"{VOLUME}_sale", f"{VOLUME}_vehicle", VOLUME}
    assert len(groups[f"{VOLUME}_sale"]) == 2
    assert len(groups[f"{VOLUME}_vehicle"]) == 1
    assert len(groups[VOLUME]) == 1


def test_batch_vacio_no_produce_grupos():
    assert group_by_volume([]) == {}

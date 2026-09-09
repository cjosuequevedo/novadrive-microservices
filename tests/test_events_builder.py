"""Verifica build_event() contra la tabla before/after de la seccion 11."""

from datetime import datetime, timezone

import pytest

from common.events.builder import EventContractError, build_event


CORR_ID = "9f2b3b3a-0000-4a1a-8b1a-000000000001"


def test_creacion_c_exige_before_none_y_after_presente():
    ev = build_event(
        entity="customer",
        operation="c",
        record_key={"CUSTOMER_CODE": "CUS0001"},
        before=None,
        after={"CUSTOMER_CODE": "CUS0001", "FULL_NAME": "Alex Rivera"},
        correlation_id=CORR_ID,
    )
    assert ev.operation == "c"
    assert ev.before is None
    assert ev.after == {"CUSTOMER_CODE": "CUS0001", "FULL_NAME": "Alex Rivera"}
    assert ev.entity == "customer"
    assert ev.source_table == "ND_CUSTOMER"
    assert ev.source_system == "NOVADRIVE"
    assert ev.event_version == 1
    # event_id debe ser un UUID valido y distinto en cada llamada
    assert len(ev.event_id) == 36


def test_creacion_c_con_before_no_none_falla():
    with pytest.raises(EventContractError):
        build_event(
            entity="customer",
            operation="c",
            record_key={"CUSTOMER_CODE": "CUS0001"},
            before={"CUSTOMER_CODE": "CUS0001"},  # invalido para 'c'
            after={"CUSTOMER_CODE": "CUS0001"},
            correlation_id=CORR_ID,
        )


def test_actualizacion_u_exige_before_y_after():
    ev = build_event(
        entity="vehicle",
        operation="u",
        record_key={"CHASSIS_NUMBER": "1HGCM82633A004352"},
        before={"CHASSIS_NUMBER": "1HGCM82633A004352", "AVAILABILITY_CODE": "AVL"},
        after={"CHASSIS_NUMBER": "1HGCM82633A004352", "AVAILABILITY_CODE": "SOLD"},
        correlation_id=CORR_ID,
    )
    assert ev.before["AVAILABILITY_CODE"] == "AVL"
    assert ev.after["AVAILABILITY_CODE"] == "SOLD"


@pytest.mark.parametrize("before,after", [(None, {"x": 1}), ({"x": 1}, None), (None, None)])
def test_actualizacion_u_sin_before_o_sin_after_falla(before, after):
    with pytest.raises(EventContractError):
        build_event(
            entity="vehicle",
            operation="u",
            record_key={"CHASSIS_NUMBER": "1HGCM82633A004352"},
            before=before,
            after=after,
            correlation_id=CORR_ID,
        )


def test_eliminacion_d_exige_after_none_y_before_presente():
    ev = build_event(
        entity="sale",
        operation="d",
        record_key={"ORDER_NUMBER": "ORD-0001"},
        before={"ORDER_NUMBER": "ORD-0001", "ORDER_STATUS": "OPEN"},
        after=None,
        correlation_id=CORR_ID,
    )
    assert ev.after is None
    assert ev.before["ORDER_STATUS"] == "OPEN"


def test_eliminacion_d_con_after_no_none_falla():
    with pytest.raises(EventContractError):
        build_event(
            entity="sale",
            operation="d",
            record_key={"ORDER_NUMBER": "ORD-0001"},
            before={"ORDER_NUMBER": "ORD-0001"},
            after={"ORDER_NUMBER": "ORD-0001"},  # invalido para 'd'
            correlation_id=CORR_ID,
        )


def test_entidad_desconocida_falla():
    with pytest.raises(EventContractError):
        build_event(
            entity="invoice",  # type: ignore[arg-type]
            operation="c",
            record_key={},
            before=None,
            after={},
            correlation_id=CORR_ID,
        )


def test_occurred_at_por_defecto_es_utc_ahora():
    antes = datetime.now(timezone.utc)
    ev = build_event(
        entity="customer",
        operation="c",
        record_key={"CUSTOMER_CODE": "CUS0001"},
        before=None,
        after={"CUSTOMER_CODE": "CUS0001"},
        correlation_id=CORR_ID,
    )
    despues = datetime.now(timezone.utc)
    assert antes <= ev.occurred_at <= despues
    assert ev.occurred_at.tzinfo is not None


def test_dos_eventos_del_mismo_correlation_id_tienen_event_id_distintos():
    # Caso de la seccion 11: una orden puede producir sale + vehicle con
    # el mismo correlation_id pero cada uno con su propio event_id.
    ev_sale = build_event(
        entity="sale", operation="c", record_key={"ORDER_NUMBER": "ORD-0001"},
        before=None, after={"ORDER_NUMBER": "ORD-0001"}, correlation_id=CORR_ID,
    )
    ev_vehicle = build_event(
        entity="vehicle", operation="u", record_key={"CHASSIS_NUMBER": "1HGCM82633A004352"},
        before={"AVAILABILITY_CODE": "AVL"}, after={"AVAILABILITY_CODE": "SOLD"}, correlation_id=CORR_ID,
    )
    assert ev_sale.correlation_id == ev_vehicle.correlation_id == CORR_ID
    assert ev_sale.event_id != ev_vehicle.event_id

"""Verifica que los importes Decimal se serialicen sin pasar por float
(seccion 11: 'Serializar importes Decimal sin float')."""

import json
from datetime import datetime, timezone
from decimal import Decimal

from common.events.serialization import dumps_decimal_safe


def test_decimal_se_serializa_como_string_no_como_numero_json():
    payload = {"net_amount": Decimal("23275.00")}
    salida = dumps_decimal_safe(payload)
    # Debe aparecer como STRING JSON ("23275.00"), no como numero desnudo (23275.00)
    assert '"net_amount": "23275.00"' in salida


def test_decimal_clasico_trampa_de_float_no_pierde_precision():
    # 0.1 + 0.2 en float da 0.30000000000000004 - con Decimal exacto debe
    # conservarse "0.30" tal cual, sin arrastrar ese error de redondeo.
    valor = Decimal("0.1") + Decimal("0.2")
    assert str(valor) == "0.3"

    salida = dumps_decimal_safe({"monto": valor})
    parsed = json.loads(salida)
    # Como via string, json.loads lo trae de vuelta como str exacto, no float
    assert parsed["monto"] == "0.3"
    assert isinstance(parsed["monto"], str)


def test_decimal_de_16_2_precision_exacta_no_se_trunca():
    # Simula NET_AMOUNT NUMERIC(16,2) con un valor donde float SI perderia precision
    valor = Decimal("123456789012.34")
    salida = dumps_decimal_safe({"total": valor})
    parsed = json.loads(salida)
    assert parsed["total"] == "123456789012.34"


def test_discount_pct_numeric_7_4_conserva_los_4_decimales():
    # DISCOUNT_PCT es NUMERIC(7,4) - "10.5000" (puntos porcentuales, no
    # fraccion) debe sobrevivir intacto, no redondearse a "10.5".
    valor = Decimal("10.5000")
    salida = dumps_decimal_safe({"discount_pct": valor})
    parsed = json.loads(salida)
    assert parsed["discount_pct"] == "10.5000"


def test_datetime_se_serializa_iso8601_utc():
    dt = datetime(2026, 9, 9, 12, 30, 0, tzinfo=timezone.utc)
    salida = dumps_decimal_safe({"occurred_at": dt})
    parsed = json.loads(salida)
    assert parsed["occurred_at"] == "2026-09-09T12:30:00+00:00"


def test_datetime_naive_se_asume_utc():
    dt_naive = datetime(2026, 9, 9, 12, 30, 0)  # sin tzinfo
    salida = dumps_decimal_safe({"occurred_at": dt_naive})
    parsed = json.loads(salida)
    assert parsed["occurred_at"] == "2026-09-09T12:30:00+00:00"


def test_tipo_no_soportado_lanza_typeerror():
    class NoSerializable:
        pass

    try:
        dumps_decimal_safe({"x": NoSerializable()})
        assert False, "debia lanzar TypeError"
    except TypeError:
        pass

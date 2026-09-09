"""Verifica los tipos Pydantic reutilizables de common/schemas/validators.py
- nacen del estandar de interfaz del 9 sep 2026 ("validacion de
negativos... desde el primer momento, no como bug encontrado
despues"), asi que se prueban aca antes de que cualquier ruta los use."""

from decimal import Decimal

import pytest
from pydantic import BaseModel, ValidationError

from common.schemas.validators import DiscountPercentage, NonNegativeAmount


class _AmountModel(BaseModel):
    amount: NonNegativeAmount


class _DiscountModel(BaseModel):
    pct: DiscountPercentage


def test_monto_negativo_rechazado():
    with pytest.raises(ValidationError):
        _AmountModel(amount=Decimal("-0.01"))


def test_monto_cero_permitido():
    # 0 es el limite inferior valido (ej. TAX_AMOUNT sin impuesto)
    modelo = _AmountModel(amount=Decimal("0"))
    assert modelo.amount == Decimal("0")


def test_monto_positivo_permitido():
    modelo = _AmountModel(amount=Decimal("24500.00"))
    assert modelo.amount == Decimal("24500.00")


def test_descuento_negativo_rechazado():
    with pytest.raises(ValidationError):
        _DiscountModel(pct=Decimal("-0.01"))


def test_descuento_mayor_a_100_rechazado():
    with pytest.raises(ValidationError):
        _DiscountModel(pct=Decimal("100.01"))


@pytest.mark.parametrize("valor", [Decimal("0"), Decimal("10.5000"), Decimal("100")])
def test_descuento_en_rango_valido_permitido(valor):
    modelo = _DiscountModel(pct=valor)
    assert modelo.pct == valor


def test_error_de_validacion_es_serializable_a_json_via_jsonable_encoder():
    # Reproduce el hallazgo #13 de Andes (CLAUDE.md de Andes): un
    # ValidationError con un valor Decimal invalido en el detalle no
    # debe reventar al intentar convertirlo a JSON. FastAPI usa
    # jsonable_encoder(exc.errors()) en las rutas reales (Fase 4) - acá
    # se verifica el mismo mecanismo en aislado, sin FastAPI de por medio.
    from fastapi.encoders import jsonable_encoder
    import json

    try:
        _AmountModel(amount=Decimal("-500"))
        assert False, "debia lanzar ValidationError"
    except ValidationError as exc:
        encoded = jsonable_encoder(exc.errors())
        # No debe lanzar TypeError al serializar (el bug real de Andes
        # era exactamente esto: exc.errors() crudo con un Decimal adentro
        # rompia el JSONResponse por defecto de FastAPI con un 500).
        serialized = json.dumps(encoded)
        assert "-500" in serialized

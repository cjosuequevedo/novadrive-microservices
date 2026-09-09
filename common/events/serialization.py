"""
serialization.py - JSON "Decimal sin float" (seccion 11 del documento).

Misma decision de diseno que Andes (contrato compartido, ver
LECCIONES_TRANSVERSALES.md y CLAUDE.md "Contrato de eventos"): los
Decimal se serializan como STRING JSON (ej. "23275.00"), no como
numero JSON desnudo. Un numero JSON desnudo puede volver a pasar por
float en cualquier lector que no sepa que ese campo es dinero
(json.loads de Python, JSON.parse de JS, y Bronze/Spark tambien si no
se castea explicito) - una cadena obliga al consumidor a parsear
explicitamente como Decimal/BigDecimal.

Los datetime se serializan como ISO 8601 en UTC.
"""

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.astimezone(timezone.utc).isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Objeto no serializable a JSON en el contrato de eventos: {type(obj)!r}")


def dumps_decimal_safe(data: Any) -> str:
    """json.dumps determinista (claves ordenadas) sin pasar Decimal por float."""
    return json.dumps(data, default=_json_default, ensure_ascii=False, sort_keys=True)

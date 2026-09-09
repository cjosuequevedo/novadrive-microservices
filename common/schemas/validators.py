"""
validators.py - Tipos Pydantic reutilizables para los campos
monetarios/porcentuales de los tres microservicios (Customer,
Inventory, Order).

Nace directamente del estandar de interfaz acordado el 9 sep 2026
(CLAUDE.md, "Validacion de negativos en campos monetarios/
porcentuales desde el primer momento, no como bug encontrado
despues") - Andes encontro ese bug ya tarde (ver su propio CLAUDE.md,
hallazgo #13: un ValidationError con un valor Decimal invalido
reventaba en 500 en vez de 422, porque el JSONResponse default de
FastAPI no sabe serializar Decimal dentro de exc.errors()). Definir
estos tipos acá, en un solo lugar de `common`, evita que cada
microservicio reinvente su propio `Field(ge=0)` de forma inconsistente
- y el manejo de excepcion con `jsonable_encoder` (para no repetir el
bug #13 de Andes) se centraliza en `common/errors.py` cuando se
conecte a las rutas FastAPI de cada servicio (Fase 4, no este
checkpoint).
"""

from decimal import Decimal
from typing import Annotated

from pydantic import Field

# Un monto monetario (GROSS_AMOUNT, NET_AMOUNT, LIST_AMOUNT, CAPTURED_AMOUNT, ...)
# nunca puede ser negativo - ver CHECK constraints de db/ddl_postgres.sql,
# que ya rechazan esto a nivel de base. Validarlo tambien acá evita un
# viaje redondo a Postgres solo para enterarse de un dato invalido.
NonNegativeAmount = Annotated[Decimal, Field(ge=0)]

# DISCOUNT_PCT / LINE_DISCOUNT_PCT: puntos porcentuales, 0 a 100 (ver
# decision de diseno #6 en CLAUDE.md - "10.5" significa 10.5%, no "0.105").
DiscountPercentage = Annotated[Decimal, Field(ge=0, le=100)]

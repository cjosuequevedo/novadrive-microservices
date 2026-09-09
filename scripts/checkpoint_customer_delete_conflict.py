"""
checkpoint_customer_delete_conflict.py - Fixture desechable (no runtime,
no parte del flujo de demo) para poder verificar con evidencia real la
regla "no eliminar customer con ordenes" (seccion 9) ANTES de que el
microservicio de Orders exista todavia.

Crea un customer real via el microservicio (HTTP real), y una
ND_SALES_ORDER minima directo por ORM (unica excepcion deliberada a
"no ejecutar DML manual" - es preparacion de un caso de prueba de
desarrollo, no la demostracion en si, que no tiene todavia el
microservicio de Orders para generar la orden por su propio POST).
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import httpx

from common.db import SessionLocal
from common.models import Branch, InventoryUnit, SalesAgent, SalesOrder

AUTH = ("admin", "changeme")
BASE = "http://127.0.0.1:8001"


def main() -> None:
    # 1) Customer real, via HTTP real (no DML manual para esta parte)
    resp = httpx.post(
        f"{BASE}/customers",
        data={"document_number": "DOC-NOVA-LINKED", "full_name": "Riley Ortiz"},
        auth=AUTH,
    )
    resp.raise_for_status()
    customer_code = resp.json()["data"]["customer_code"]
    print(f"OK customer real creado via HTTP: {customer_code}")

    # 2) Fixture minimo de referencia + orden, directo por ORM (excepcion
    #    deliberada, ver docstring - Orders no tiene microservicio todavia).
    session = SessionLocal()
    try:
        branch = Branch(branch_code="BR99", display_name="Fixture Branch", city_name="Testville")
        agent = SalesAgent(agent_code="AG99", branch_code="BR99", agent_display_name="Fixture Agent")
        unit = InventoryUnit(
            chassis_number="FIXTURECHASSIS001",
            branch_code="BR99",
            brand_name="FixtureBrand",
            model_name="FixtureModel",
            model_year=2024,
            list_amount=Decimal("10000.00"),
        )
        session.add_all([branch, agent, unit])
        session.flush()

        order = SalesOrder(
            order_number=f"ORD-FIXTURE-{uuid.uuid4().hex[:8].upper()}",
            buyer_code=customer_code,
            sales_agent_code="AG99",
            fulfillment_branch="BR99",
            ordered_at=datetime.now(timezone.utc).replace(tzinfo=None),
            gross_amount=Decimal("10000.00"),
            net_amount=Decimal("10000.00"),
        )
        session.add(order)
        session.commit()
        print(f"OK orden fixture creada por ORM: {order.order_number} -> buyer_code={customer_code}")
    finally:
        session.close()

    # 3) Intentar eliminar el customer via HTTP real -> debe dar 409
    resp = httpx.post(f"{BASE}/customers/{customer_code}/delete", auth=AUTH)
    print(f"DELETE /customers/{customer_code}/delete -> HTTP {resp.status_code}")
    print(resp.json())
    assert resp.status_code == 409, f"se esperaba 409, se obtuvo {resp.status_code}"
    print("OK: 409 real confirmado, customer con ordenes no se pudo eliminar")

    # 4) Limpieza: borrar el fixture directo por ORM (mismo motivo que el paso 2)
    session = SessionLocal()
    try:
        session.query(SalesOrder).filter_by(buyer_code=customer_code).delete()
        session.query(InventoryUnit).filter_by(chassis_number="FIXTURECHASSIS001").delete()
        session.query(SalesAgent).filter_by(agent_code="AG99").delete()
        session.query(Branch).filter_by(branch_code="BR99").delete()
        session.commit()
    finally:
        session.close()

    # 5) Ahora si debe poder eliminarse via HTTP real
    resp = httpx.post(f"{BASE}/customers/{customer_code}/delete", auth=AUTH)
    print(f"DELETE (tras limpiar la orden) -> HTTP {resp.status_code}")
    assert resp.status_code == 200, f"se esperaba 200, se obtuvo {resp.status_code}"
    print("OK: tras liberar la orden, el mismo DELETE ahora da 200")


if __name__ == "__main__":
    main()

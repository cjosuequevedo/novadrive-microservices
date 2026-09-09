"""
checkpoint_inventory_delete_conflict.py - Fixture desechable (no
runtime, no parte del flujo de demo) para verificar con evidencia real
la regla "no eliminar unidad ya vinculada a una orden" (seccion 9)
ANTES de que el microservicio de Orders exista todavia.

Misma excepcion deliberada que
scripts/checkpoint_customer_delete_conflict.py: la orden/item se crea
directo por ORM porque Orders no tiene microservicio propio todavia -
documentado aqui, no oculto.
"""

from datetime import datetime, timezone
from decimal import Decimal

import httpx

from common.db import SessionLocal
from common.models import Customer, OrderItem, SalesAgent, SalesOrder

AUTH = ("admin", "changeme")
INVENTORY_BASE = "http://127.0.0.1:8002"


def main() -> None:
    chassis = "9HGCM82633A004399"

    # 1) Inventory unit real, via HTTP real
    resp = httpx.post(
        f"{INVENTORY_BASE}/inventory",
        data={
            "branch_code": "BR01",
            "chassis_number": chassis,
            "brand_name": "Toyota",
            "model_name": "Camry",
            "model_year": "2023",
            "list_amount": "22000.00",
        },
        auth=AUTH,
    )
    resp.raise_for_status()
    print(f"OK inventory unit real creada via HTTP: {chassis}")

    # 2) Fixture minimo (customer + agent + order + order_item), directo
    #    por ORM - ver docstring.
    session = SessionLocal()
    try:
        customer = Customer(customer_code="CUS-FIXTURE01", document_number="DOC-FIXTURE-INV", full_name="Fixture Buyer")
        agent = SalesAgent(agent_code="AG-FIX01", branch_code="BR01", agent_display_name="Fixture Agent 2")
        session.add_all([customer, agent])
        session.flush()

        order = SalesOrder(
            order_number="ORD-FIXTURE-INV01",
            buyer_code=customer.customer_code,
            sales_agent_code=agent.agent_code,
            fulfillment_branch="BR01",
            ordered_at=datetime.now(timezone.utc).replace(tzinfo=None),
            gross_amount=Decimal("22000.00"),
            net_amount=Decimal("22000.00"),
        )
        session.add(order)
        session.flush()

        item = OrderItem(
            order_number=order.order_number,
            line_number=1,
            chassis_number=chassis,
            unit_gross_amount=Decimal("22000.00"),
            unit_net_amount=Decimal("22000.00"),
        )
        session.add(item)
        session.commit()
        print(f"OK order_item fixture creado por ORM: {order.order_number} -> chassis={chassis}")
    finally:
        session.close()

    # 3) Intentar eliminar la unidad via HTTP real -> debe dar 409
    resp = httpx.post(f"{INVENTORY_BASE}/inventory/{chassis}/delete", auth=AUTH)
    print(f"DELETE /inventory/{chassis}/delete -> HTTP {resp.status_code}")
    print(resp.json())
    assert resp.status_code == 409, f"se esperaba 409, se obtuvo {resp.status_code}"
    print("OK: 409 real confirmado, unidad vinculada a una orden no se pudo eliminar")

    # 4) Limpieza del fixture
    session = SessionLocal()
    try:
        session.query(OrderItem).filter_by(order_number="ORD-FIXTURE-INV01").delete()
        session.query(SalesOrder).filter_by(order_number="ORD-FIXTURE-INV01").delete()
        session.query(SalesAgent).filter_by(agent_code="AG-FIX01").delete()
        session.query(Customer).filter_by(customer_code="CUS-FIXTURE01").delete()
        session.commit()
    finally:
        session.close()

    # 5) Ahora si debe poder eliminarse via HTTP real
    resp = httpx.post(f"{INVENTORY_BASE}/inventory/{chassis}/delete", auth=AUTH)
    print(f"DELETE (tras liberar el item) -> HTTP {resp.status_code}")
    assert resp.status_code == 200, f"se esperaba 200, se obtuvo {resp.status_code}"
    print("OK: tras liberar el order_item, el mismo DELETE ahora da 200")


if __name__ == "__main__":
    main()

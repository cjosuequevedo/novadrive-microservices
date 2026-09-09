"""
checkpoint_order_captured_payment_conflict.py - Verifica con evidencia
real la regla "no eliminar si tiene un pago capturado" (seccion 9,
"Eliminacion de venta": "sin pago capturado"). Crea la orden con pago
real via HTTP, y solo marca `captured_at` (el momento de la
confirmacion del cobro, que ningun flujo de creacion pone
automaticamente todavia porque no hay integracion real de pagos en
este MVP) directo por ORM - unica excepcion deliberada, documentada,
igual criterio que los otros checkpoint_*_delete_conflict.py.
"""

from datetime import datetime, timezone

import httpx

from common.db import SessionLocal
from common.models import PaymentTxn

AUTH = ("admin", "changeme")
INVENTORY_BASE = "http://127.0.0.1:8002"
ORDERS_BASE = "http://127.0.0.1:8003"


def main() -> None:
    chassis = "6HGCM82633A004366"
    r = httpx.post(
        f"{INVENTORY_BASE}/inventory",
        data={
            "branch_code": "BR01", "chassis_number": chassis, "brand_name": "Ford",
            "model_name": "Escape", "model_year": "2024", "list_amount": "25000.00",
        },
        auth=AUTH,
    )
    r.raise_for_status()
    print(f"OK inventory unit real: {chassis}")

    r = httpx.post(
        f"{ORDERS_BASE}/orders",
        data={
            "buyer_code": "CUS-71FF845579", "sales_agent_code": "AG01", "fulfillment_branch": "BR01",
            "chassis_number": chassis, "payment_channel": "CARD", "captured_amount": "25000.00",
            "payment_reference": "PAY-CAPTURED-001",
        },
        auth=AUTH,
    )
    r.raise_for_status()
    order_number = r.json()["data"]["order_number"]
    print(f"OK order real con pago real: {order_number}")

    # Marcar el pago como capturado (excepcion deliberada, ver docstring)
    session = SessionLocal()
    try:
        payment = session.get(PaymentTxn, "PAY-CAPTURED-001")
        payment.captured_at = datetime.now(timezone.utc).replace(tzinfo=None)
        session.commit()
        print("OK payment_txn.captured_at seteado por ORM (fixture, no flujo real de pagos)")
    finally:
        session.close()

    resp = httpx.post(f"{ORDERS_BASE}/orders/{order_number}/delete", auth=AUTH)
    print(f"DELETE con pago capturado -> HTTP {resp.status_code}")
    print(resp.json())
    assert resp.status_code == 409, f"se esperaba 409, se obtuvo {resp.status_code}"
    print("OK: 409 real confirmado, orden con pago capturado no se pudo eliminar")


if __name__ == "__main__":
    main()

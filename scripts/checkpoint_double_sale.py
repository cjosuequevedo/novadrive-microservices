"""
checkpoint_double_sale.py - Verificacion real (no solo el codigo HTTP)
de que una doble venta sobre el MISMO chasis, disparada con
concurrencia real (dos hilos, no secuencial), produce exactamente UNA
orden y CERO cambio parcial - seccion 9 / prueba obligatoria de la
seccion 21 del PDF ("Intentar doble venta -> Respuesta 409 sin cambio
parcial").

Usa threading.Barrier para que ambas requests salgan lo mas
simultaneamente posible (no una tras otra) - la garantia real viene del
`FOR UPDATE` sobre `nd.inventory_unit` en
services.orders.repository.get_inventory_unit_for_update, no de la
suerte de que una request termine antes que la otra empiece.
"""

import threading

import httpx

from common.db import SessionLocal
from common.models import InventoryUnit, OrderItem, SalesOrder

AUTH = ("admin", "changeme")
CUSTOMERS_BASE = "http://127.0.0.1:8001"
INVENTORY_BASE = "http://127.0.0.1:8002"
ORDERS_BASE = "http://127.0.0.1:8003"

CHASSIS = "4HGCM82633A004344"


def _count(model, **filters) -> int:
    session = SessionLocal()
    try:
        query = session.query(model)
        for k, v in filters.items():
            query = query.filter(getattr(model, k) == v)
        return query.count()
    finally:
        session.close()


def main() -> None:
    # 1) Setup real: dos customers distintos + una unidad AVL compartida
    buyers = []
    for i in (1, 2):
        r = httpx.post(
            f"{CUSTOMERS_BASE}/customers",
            data={"document_number": f"DOC-DBLSALE-{i}", "full_name": f"Double Sale Buyer {i}"},
            auth=AUTH,
        )
        r.raise_for_status()
        buyers.append(r.json()["data"]["customer_code"])
    print(f"OK 2 customers reales creados: {buyers}")

    r = httpx.post(
        f"{INVENTORY_BASE}/inventory",
        data={
            "branch_code": "BR01", "chassis_number": CHASSIS, "brand_name": "Kia",
            "model_name": "Sportage", "model_year": "2025", "list_amount": "27000.00",
        },
        auth=AUTH,
    )
    r.raise_for_status()
    print(f"OK inventory unit AVL real creada: {CHASSIS}")

    count_before = _count(SalesOrder, fulfillment_branch="BR01") if False else None  # no usado, ver conteo real abajo
    orders_before = _count(SalesOrder)
    items_before = _count(OrderItem, chassis_number=CHASSIS)
    print(f"Conteo ANTES: sales_order total={orders_before}, order_item para {CHASSIS}={items_before}")

    # 2) Disparo concurrente real: dos threads sincronizados con Barrier,
    #    cada uno intentando comprar el MISMO chasis para un buyer distinto.
    barrier = threading.Barrier(2)
    results: list[httpx.Response] = [None, None]  # type: ignore[list-item]

    def attempt(idx: int, buyer_code: str) -> None:
        barrier.wait()  # ambos threads pasan la barrera casi al mismo instante
        results[idx] = httpx.post(
            f"{ORDERS_BASE}/orders",
            data={
                "buyer_code": buyer_code, "sales_agent_code": "AG01", "fulfillment_branch": "BR01",
                "chassis_number": CHASSIS,
            },
            auth=AUTH,
            timeout=30,
        )

    t1 = threading.Thread(target=attempt, args=(0, buyers[0]))
    t2 = threading.Thread(target=attempt, args=(1, buyers[1]))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    statuses = [r.status_code for r in results]
    bodies = [r.json() for r in results]
    print(f"Status codes de los 2 intentos concurrentes: {statuses}")
    for i, b in enumerate(bodies):
        print(f"  intento {i}: {b}")

    # 3) Evidencia real: exactamente un 200 y un 409, nunca dos 200 ni dos 409
    assert sorted(statuses) == [200, 409], f"se esperaba [200, 409], se obtuvo {sorted(statuses)}"
    print("OK: exactamente un 200 y un 409 (no dos ventas, no cero ventas)")

    # 4) Conteo DESPUES - la evidencia real que importa, no solo el status code
    orders_after = _count(SalesOrder)
    items_after = _count(OrderItem, chassis_number=CHASSIS)
    print(f"Conteo DESPUES: sales_order total={orders_after}, order_item para {CHASSIS}={items_after}")

    assert orders_after == orders_before + 1, (
        f"se esperaba exactamente +1 orden nueva, orders_before={orders_before} orders_after={orders_after}"
    )
    assert items_after == items_before + 1, (
        f"se esperaba exactamente +1 order_item para el chasis, items_before={items_before} items_after={items_after}"
    )
    print("OK: el conteo real confirma +1 orden y +1 item - sin cambio parcial ni doble venta")

    # 5) La unidad debe haber quedado SOLD (una sola vez, consistente)
    session = SessionLocal()
    try:
        unit = session.get(InventoryUnit, CHASSIS)
        print(f"availability_code final de {CHASSIS}: {unit.availability_code}")
        assert unit.availability_code == "SOLD"
    finally:
        session.close()
    print("OK: inventory unit terminó SOLD, sin quedar en un estado intermedio inconsistente")


if __name__ == "__main__":
    main()

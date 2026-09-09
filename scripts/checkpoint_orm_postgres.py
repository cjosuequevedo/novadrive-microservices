"""
checkpoint_orm_postgres.py - Evidencia real (no "deberia funcionar") de
que los 8 modelos ORM de common/models/ mapean correctamente el schema
`nd` ya aplicado en Postgres (ver CLAUDE.md, Fase 1).

Script desechable de verificacion (mismo espiritu que
scripts/checkpoint_*.py en Andes) - no es parte del runtime de la app.
Requiere el Postgres real de docker-compose.local.yml corriendo.

Ejercita, contra la base real:
1. INSERT de las 8 tablas en orden de dependencia (branch -> ... -> outbox_event).
2. SELECT de vuelta, incluida navegacion por relationship() (branch.sales_agents, etc).
3. Un UPDATE real (transicion de estado de la orden).
4. Un intento de violar una CHECK constraint via ORM (debe fallar con IntegrityError,
   no en silencio) y confirmar que el rollback deja la sesion utilizable de nuevo.
5. DELETE en orden inverso, dejando el schema limpio otra vez.
"""

import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy.exc import IntegrityError

from common.db import SessionLocal, engine
from common.models import (
    Branch,
    Customer,
    InventoryUnit,
    OrderItem,
    OutboxEvent,
    PaymentTxn,
    SalesAgent,
    SalesOrder,
)


def main() -> None:
    print(f"Engine real: {engine.url.render_as_string(hide_password=True)}")

    session = SessionLocal()
    try:
        # --- 1. INSERT de las 8 tablas, respetando dependencias ---
        branch = Branch(branch_code="BR01", display_name="NovaDrive Downtown", city_name="Metropolis")
        agent = SalesAgent(agent_code="AG01", branch_code="BR01", agent_display_name="Jordan Vega")
        customer = Customer(customer_code="CUS0001", document_number="DOC-001", full_name="Alex Rivera")
        unit = InventoryUnit(
            chassis_number="1HGCM82633A004352",
            branch_code="BR01",
            brand_name="Honda",
            model_name="Civic",
            model_year=2024,
            list_amount=Decimal("24500.00"),
        )
        session.add_all([branch, agent, customer, unit])
        session.flush()  # sin commit todavia: existencia real garantizada por FKs abajo
        print("OK insert branch/agent/customer/inventory_unit (flush, sin commit aun)")

        order = SalesOrder(
            order_number="ORD-0001",
            buyer_code="CUS0001",
            sales_agent_code="AG01",
            fulfillment_branch="BR01",
            gross_amount=Decimal("24500.00"),
            discount_pct=Decimal("5.0"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("23275.00"),
        )
        session.add(order)
        session.flush()

        item = OrderItem(
            order_number="ORD-0001",
            line_number=1,
            chassis_number="1HGCM82633A004352",
            unit_gross_amount=Decimal("24500.00"),
            line_discount_pct=Decimal("5.0"),
            unit_net_amount=Decimal("23275.00"),
        )
        payment = PaymentTxn(
            payment_reference="PAY-0001",
            order_number="ORD-0001",
            payment_channel="CARD",
            captured_amount=Decimal("23275.00"),
        )
        session.add_all([item, payment])

        outbox_event_id = str(uuid.uuid4())
        correlation_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        outbox = OutboxEvent(
            event_id=outbox_event_id,
            source_system="NOVADRIVE",
            entity_name="sale",
            source_table="ND_SALES_ORDER",
            operation="c",
            record_key_json='{"order_number": "ORD-0001"}',
            kafka_topic="cdc.novadrive.sale",
            kafka_key="NOVADRIVE|sale|ORD-0001",
            payload_json="{}",
            next_retry_at=now,
            occurred_at=now,
            created_at=now,
            correlation_id=correlation_id,
        )
        session.add(outbox)
        session.commit()
        print("OK commit real: order + item + payment + outbox_event")

        # --- 2. SELECT de vuelta + navegacion por relationship() ---
        session.expire_all()
        fetched_branch = session.get(Branch, "BR01")
        assert fetched_branch is not None
        assert len(fetched_branch.sales_agents) == 1
        assert len(fetched_branch.inventory_units) == 1
        assert fetched_branch.sales_agents[0].agent_code == "AG01"
        print(f"OK select + relationship: {fetched_branch!r} -> agents={[a.agent_code for a in fetched_branch.sales_agents]}")

        fetched_order = session.get(SalesOrder, "ORD-0001")
        assert fetched_order is not None
        assert fetched_order.customer.full_name == "Alex Rivera"
        assert fetched_order.items[0].chassis_number == "1HGCM82633A004352"
        assert fetched_order.items[0].inventory_unit.brand_name == "Honda"
        print(f"OK select + relationship cruzada: {fetched_order!r} -> customer={fetched_order.customer.full_name!r}")

        fetched_outbox = session.query(OutboxEvent).filter_by(event_id=outbox_event_id).one()
        assert fetched_outbox.status == "PENDING"
        print(f"OK select outbox: {fetched_outbox!r}")

        # --- 3. UPDATE real (transicion de estado) ---
        fetched_order.order_status = "APPROVED"
        session.commit()
        session.expire_all()
        reloaded = session.get(SalesOrder, "ORD-0001")
        assert reloaded.order_status == "APPROVED"
        print(f"OK update real: order_status -> {reloaded.order_status!r}")

        # --- 4. Violar una CHECK constraint via ORM a proposito ---
        bad_order = SalesOrder(
            order_number="ORD-BAD",
            buyer_code="CUS0001",
            sales_agent_code="AG01",
            fulfillment_branch="BR01",
            gross_amount=Decimal("-100.00"),
            net_amount=Decimal("0"),
        )
        session.add(bad_order)
        try:
            session.commit()
            print("FALLO: se esperaba IntegrityError por gross_amount negativo y no ocurrio")
            sys.exit(1)
        except IntegrityError as exc:
            session.rollback()
            assert "ck_sales_order_gross_amount" in str(exc.orig)
            print(f"OK CHECK constraint via ORM: rollback real tras IntegrityError ({exc.orig})")

        # La sesion debe seguir utilizable despues del rollback.
        still_there = session.get(SalesOrder, "ORD-0001")
        assert still_there is not None and still_there.order_status == "APPROVED"
        print("OK sesion utilizable despues del rollback (dato bueno sigue intacto)")

        # --- 5. DELETE en orden inverso, dejar el schema limpio ---
        session.delete(fetched_outbox)
        session.delete(payment)
        session.delete(item)
        session.delete(order)
        session.delete(unit)
        session.delete(customer)
        session.delete(agent)
        session.delete(branch)
        session.commit()

        remaining = session.query(SalesOrder).count()
        assert remaining == 0
        print("OK delete en cascada manual: schema `nd` limpio de nuevo")

    finally:
        session.close()

    print("\nCHECKPOINT ORM POSTGRES: TODO OK (evidencia real arriba, sin asumir nada)")


if __name__ == "__main__":
    main()

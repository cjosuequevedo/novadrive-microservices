"""Acceso a datos de nd.sales_order / nd.order_item / nd.payment_txn.

Este microservicio SI escribe fuera de sus propias tablas (nd.
inventory_unit.availability_code) dentro de su propia transaccion - es
la excepcion documentada en CLAUDE.md decision #2: la atomicidad de
"un solo commit" que exige la seccion 9 no es alcanzable entre bases
separadas por servicio sin una saga, explicitamente fuera de alcance.
Por eso lee y bloquea `InventoryUnit` (con FOR UPDATE) directo, en vez
de llamar por HTTP al microservicio de Inventory."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from common.models.branch import Branch
from common.models.customer import Customer
from common.models.inventory_unit import InventoryUnit
from common.models.order_item import OrderItem
from common.models.payment_txn import PaymentTxn
from common.models.sales_agent import SalesAgent
from common.models.sales_order import SalesOrder


def get(session: Session, order_number: str) -> SalesOrder | None:
    return session.get(SalesOrder, order_number)


def get_for_update(session: Session, order_number: str) -> SalesOrder | None:
    stmt = select(SalesOrder).where(SalesOrder.order_number == order_number).with_for_update()
    return session.execute(stmt).scalar_one_or_none()


def get_item(session: Session, order_number: str) -> OrderItem | None:
    stmt = select(OrderItem).where(OrderItem.order_number == order_number)
    return session.execute(stmt).scalar_one_or_none()


def get_payments(session: Session, order_number: str) -> list[PaymentTxn]:
    stmt = select(PaymentTxn).where(PaymentTxn.order_number == order_number)
    return list(session.execute(stmt).scalars())


def order_number_exists(session: Session, order_number: str) -> bool:
    return session.get(SalesOrder, order_number) is not None


def get_active_customer(session: Session, customer_code: str) -> Customer | None:
    stmt = select(Customer).where(Customer.customer_code == customer_code, Customer.customer_status == "A")
    return session.execute(stmt).scalar_one_or_none()


def get_active_agent_in_branch(session: Session, agent_code: str, branch_code: str) -> SalesAgent | None:
    stmt = select(SalesAgent).where(
        SalesAgent.agent_code == agent_code,
        SalesAgent.active_flag == 1,
        SalesAgent.branch_code == branch_code,
    )
    return session.execute(stmt).scalar_one_or_none()


def get_inventory_unit_for_update(session: Session, chassis_number: str) -> InventoryUnit | None:
    """SELECT ... FOR UPDATE - seccion 9 regla 3: 'Bloquear con FOR UPDATE
    las filas expuestas a concurrencia, especialmente el vehiculo.' Es la
    fila critica de concurrencia de todo este servicio: cubre el caso de
    doble venta (dos transacciones intentando vender el mismo chasis)."""
    stmt = select(InventoryUnit).where(InventoryUnit.chassis_number == chassis_number).with_for_update()
    return session.execute(stmt).scalar_one_or_none()


def list_orders(session: Session, limit: int = 200) -> list[SalesOrder]:
    stmt = select(SalesOrder).order_by(SalesOrder.created_on.desc()).limit(limit)
    return list(session.execute(stmt).scalars())


# --- Solo lectura, para poblar los dropdowns del formulario (nunca IDs a mano) ---


def list_enabled_branches(session: Session, limit: int = 200) -> list[Branch]:
    stmt = select(Branch).where(Branch.enabled_flag == "Y").order_by(Branch.display_name).limit(limit)
    return list(session.execute(stmt).scalars())


def list_active_customers(session: Session, limit: int = 200) -> list[Customer]:
    stmt = select(Customer).where(Customer.customer_status == "A").order_by(Customer.full_name).limit(limit)
    return list(session.execute(stmt).scalars())


def list_active_agents(session: Session, limit: int = 200) -> list[SalesAgent]:
    """Trae los agentes activos de TODAS las sedes; el formulario filtra
    en el navegador por la sede elegida (mismo patron que Andes con
    Vendedor en el formulario de Venta) - evita un viaje de red por cada
    cambio de sede."""
    stmt = (
        select(SalesAgent)
        .where(SalesAgent.active_flag == 1)
        .order_by(SalesAgent.agent_display_name)
        .limit(limit)
    )
    return list(session.execute(stmt).scalars())


def list_available_inventory(session: Session, limit: int = 200) -> list[InventoryUnit]:
    """Trae las unidades AVL de todas las sedes; el formulario filtra en
    el navegador por la sede elegida, mismo motivo que list_active_agents."""
    stmt = (
        select(InventoryUnit)
        .where(InventoryUnit.availability_code == "AVL")
        .order_by(InventoryUnit.brand_name, InventoryUnit.model_name)
        .limit(limit)
    )
    return list(session.execute(stmt).scalars())

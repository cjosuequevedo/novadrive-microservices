"""Acceso a datos de nd.inventory_unit. Sin logica de negocio aqui -
eso vive en services/inventory/service.py.

Lee `nd.branch` (dato de referencia) para poblar el dropdown, y
`nd.order_item` (de Order) para verificar "no eliminar si ya pertenece
a una orden" - lectura directa contra la base compartida, mismo
razonamiento que Customer con `nd.sales_order` (ver CLAUDE.md decision
#2: sin llamada HTTP entre microservicios para validaciones de solo
lectura)."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from common.models.branch import Branch
from common.models.inventory_unit import InventoryUnit
from common.models.order_item import OrderItem


def get(session: Session, chassis_number: str) -> InventoryUnit | None:
    return session.get(InventoryUnit, chassis_number)


def get_for_update(session: Session, chassis_number: str) -> InventoryUnit | None:
    """SELECT ... FOR UPDATE - seccion 9 regla 3: 'Bloquear con FOR UPDATE
    las filas expuestas a concurrencia, especialmente el vehiculo.'"""
    stmt = select(InventoryUnit).where(InventoryUnit.chassis_number == chassis_number).with_for_update()
    return session.execute(stmt).scalar_one_or_none()


def branch_exists_and_enabled(session: Session, branch_code: str) -> bool:
    stmt = select(Branch.branch_code).where(Branch.branch_code == branch_code, Branch.enabled_flag == "Y")
    return session.execute(stmt).first() is not None


def is_in_an_order(session: Session, chassis_number: str) -> bool:
    stmt = select(OrderItem.order_number).where(OrderItem.chassis_number == chassis_number).limit(1)
    return session.execute(stmt).first() is not None


def list_inventory(session: Session, limit: int = 200) -> list[InventoryUnit]:
    stmt = select(InventoryUnit).order_by(InventoryUnit.created_on.desc()).limit(limit)
    return list(session.execute(stmt).scalars())


def list_enabled_branches(session: Session, limit: int = 200) -> list[Branch]:
    """Solo lectura, para poblar el dropdown de Branch en el formulario -
    nunca un campo de texto libre para un branch_code que el usuario
    tendria que adivinar."""
    stmt = select(Branch).where(Branch.enabled_flag == "Y").order_by(Branch.display_name).limit(limit)
    return list(session.execute(stmt).scalars())


def list_available(session: Session, limit: int = 200) -> list[InventoryUnit]:
    """Solo lectura, para poblar el dropdown de Inventory en el
    formulario de Order (Fase 4c) - solo tiene sentido ofrecer para
    vender lo que esta AVL (order_service ya re-valida con FOR UPDATE
    de todas formas)."""
    stmt = (
        select(InventoryUnit)
        .where(InventoryUnit.availability_code == "AVL")
        .order_by(InventoryUnit.brand_name, InventoryUnit.model_name)
        .limit(limit)
    )
    return list(session.execute(stmt).scalars())

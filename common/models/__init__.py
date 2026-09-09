"""
Modelos ORM SQLAlchemy 2 de las 8 tablas ND_* (nombres fisicos en
minuscula en el schema `nd` de Postgres - ver decision de diseno #9 en
CLAUDE.md). Importados acá para que `Base.metadata` los conozca todos
al usarlos en pruebas/reflexion, sin tener que importar cada archivo
por separado en el resto del codigo.
"""

from common.models.base import Base
from common.models.branch import Branch
from common.models.customer import Customer
from common.models.sales_agent import SalesAgent
from common.models.inventory_unit import InventoryUnit
from common.models.sales_order import SalesOrder
from common.models.order_item import OrderItem
from common.models.payment_txn import PaymentTxn
from common.models.outbox_event import OutboxEvent

__all__ = [
    "Base",
    "Branch",
    "Customer",
    "SalesAgent",
    "InventoryUnit",
    "SalesOrder",
    "OrderItem",
    "PaymentTxn",
    "OutboxEvent",
]

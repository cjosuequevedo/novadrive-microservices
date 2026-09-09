"""Acceso a datos de nd.customer. Sin logica de negocio aqui - eso vive
en services/customers/service.py.

Nota de arquitectura (decision #2 en CLAUDE.md): este microservicio
lee la tabla `nd.sales_order` (de Order) para verificar la regla "no
eliminar customer con ordenes" - lectura directa contra la base
compartida, no una llamada HTTP a Orders, porque el servicio de
Customer no necesita abrir ninguna transaccion cruzada (a diferencia
de Order, que si escribe en inventory_unit dentro de su propia
transaccion)."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from common.models.customer import Customer
from common.models.sales_order import SalesOrder


def get(session: Session, customer_code: str) -> Customer | None:
    return session.get(Customer, customer_code)


def get_for_update(session: Session, customer_code: str) -> Customer | None:
    """SELECT ... FOR UPDATE - bloquea la fila para la duracion de la
    transaccion (seccion 9, regla 3: bloquear filas expuestas a
    concurrencia)."""
    stmt = select(Customer).where(Customer.customer_code == customer_code).with_for_update()
    return session.execute(stmt).scalar_one_or_none()


def document_exists(
    session: Session,
    document_number: str,
    exclude_customer_code: str | None = None,
) -> bool:
    stmt = select(Customer.customer_code).where(Customer.document_number == document_number)
    if exclude_customer_code is not None:
        stmt = stmt.where(Customer.customer_code != exclude_customer_code)
    return session.execute(stmt).first() is not None


def has_orders(session: Session, customer_code: str) -> bool:
    stmt = select(SalesOrder.order_number).where(SalesOrder.buyer_code == customer_code).limit(1)
    return session.execute(stmt).first() is not None


def code_exists(session: Session, customer_code: str) -> bool:
    return session.get(Customer, customer_code) is not None


def list_customers(session: Session, limit: int = 200) -> list[Customer]:
    stmt = select(Customer).order_by(Customer.created_on.desc()).limit(limit)
    return list(session.execute(stmt).scalars())

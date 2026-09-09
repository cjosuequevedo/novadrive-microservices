"""
seed_reference_data.py - Siembra ND_BRANCH / ND_SALES_AGENT (datos de
referencia, sin outbox - mismo criterio que Andes con
AM_SUCURSAL/AM_VENDEDOR: no generan eventos propios, ver CLAUDE.md
seccion "Modelo relacional").

Idempotente: usa merge-like manual (chequea existencia antes de
insertar) para poder correr multiples veces sin duplicar filas.
"""

from common.db import SessionLocal
from common.models import Branch, SalesAgent

BRANCHES = [
    ("BR01", "NovaDrive Downtown", "Metropolis", "100 Main Street"),
    ("BR02", "NovaDrive Uptown", "Metropolis", "200 North Avenue"),
    ("BR03", "NovaDrive Riverside", "Rivertown", "50 River Road"),
]

AGENTS = [
    ("AG01", "BR01", "Jordan Vega"),
    ("AG02", "BR01", "Casey Nguyen"),
    ("AG03", "BR02", "Morgan Blake"),
    ("AG04", "BR03", "Riley Ortiz"),
]


def main() -> None:
    session = SessionLocal()
    try:
        for code, name, city, address in BRANCHES:
            if session.get(Branch, code) is None:
                session.add(Branch(branch_code=code, display_name=name, city_name=city, street_address=address))
                print(f"+ branch {code} {name}")
            else:
                print(f"= branch {code} already exists")

        for code, branch_code, name in AGENTS:
            if session.get(SalesAgent, code) is None:
                session.add(SalesAgent(agent_code=code, branch_code=branch_code, agent_display_name=name))
                print(f"+ agent {code} {name} @ {branch_code}")
            else:
                print(f"= agent {code} already exists")

        session.commit()
        print("OK reference data seeded (idempotent)")
    finally:
        session.close()


if __name__ == "__main__":
    main()

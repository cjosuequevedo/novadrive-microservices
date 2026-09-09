"""
databricks_sql.py - Helper minimo para ejecutar SQL contra Databricks
via la Statement Execution API (submit + poll), reutilizando el mismo
SQL Warehouse serverless ya existente en el workspace (compartido con
Andes - ver CLAUDE.md decision #7: mismo workspace, catalogo propio
`novadrive_catalog`). Sin dependencias externas.

Uso: python -m bridge.databricks_sql "SELECT 1"
"""

import json
import os
import sys
import time
import urllib.request

from bridge._env import load_dotenv_if_missing

load_dotenv_if_missing()

HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
TOKEN = os.environ["DATABRICKS_TOKEN"]
# Requerido, SIN default hardcodeado - hallazgo real (9 sep 2026): antes
# caia en silencio al warehouse de Andes (548c86efc1164b8d) si esta
# variable no estaba seteada, lo cual funcionaba "por casualidad" solo
# porque hoy comparten workspace (ver CLAUDE.md, decision #7). Si el dia
# de manana NovaDrive apunta a un workspace nuevo y alguien olvida
# setear esta variable, mejor que falle rapido (KeyError explicito) a
# que consulte silenciosamente el warehouse equivocado.
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]


def _call(method: str, path: str, body: dict | None = None) -> dict:
    url = f"{HOST}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {TOKEN}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def run_sql(sql: str, wait_sec: int = 60) -> dict:
    resp = _call(
        "POST",
        "/api/2.0/sql/statements",
        {"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "0s"},
    )
    statement_id = resp["statement_id"]

    start = time.time()
    while time.time() - start < wait_sec:
        state = _call("GET", f"/api/2.0/sql/statements/{statement_id}")
        life = state["status"]["state"]
        if life in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
            return state
        time.sleep(1.5)
    raise TimeoutError(f"statement {statement_id} did not finish within {wait_sec}s")


if __name__ == "__main__":
    result = run_sql(sys.argv[1])
    state = result["status"]["state"]
    print(f"[STATE] {state}")
    if state == "FAILED":
        print(f"[ERROR] {result['status'].get('error')}")
        sys.exit(1)
    if "result" in result and result["result"].get("data_array") is not None:
        for row in result["result"]["data_array"]:
            print(row)

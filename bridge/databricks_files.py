"""
databricks_files.py - Helper minimo para subir archivos crudos a un
Volume de Unity Catalog via la Files API (PUT) - mismo patron que usa
Andes en su propio bridge (`scripts/bridge_redpanda_to_databricks.py::
subir_batch()`), sin pasar por ningun SQL Warehouse en absoluto (a
diferencia de databricks_sql.py, que si necesita uno).

Uso: python -m bridge.databricks_files <ruta-en-el-volume> <archivo-local>
"""

import os
import sys
import urllib.request

from bridge._env import load_dotenv_if_missing

load_dotenv_if_missing()

HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
TOKEN = os.environ["DATABRICKS_TOKEN"]


def upload_file(volume_path: str, content: bytes) -> None:
    """PUT crudo a la Files API. `volume_path` es la ruta completa
    dentro del Volume (ej. "/Volumes/novadrive_catalog/bronze/
    raw_events/archivo.jsonl"), no una ruta local."""
    url = f"{HOST}/api/2.0/fs/files{volume_path}"
    req = urllib.request.Request(url, data=content, method="PUT")
    req.add_header("Authorization", f"Bearer {TOKEN}")
    req.add_header("Content-Type", "application/octet-stream")
    with urllib.request.urlopen(req) as resp:
        if resp.status not in (200, 201, 204):
            raise RuntimeError(f"upload failed: HTTP {resp.status}")


if __name__ == "__main__":
    volume_path, local_path = sys.argv[1], sys.argv[2]
    with open(local_path, "rb") as f:
        data = f.read()
    upload_file(volume_path, data)
    print(f"[OK] uploaded {len(data)} bytes to {volume_path}")

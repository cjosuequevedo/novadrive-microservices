"""
_env.py - carga minima de .env (solo si la variable NO esta ya en el
entorno del proceso) para los scripts de esta carpeta que leen
DATABRICKS_HOST/DATABRICKS_TOKEN directo de os.environ. Sin
dependencias externas - mismo patron y misma razon que
scripts/_env.py de Andes (ver lección/hallazgo de seguridad en
LECCIONES_TRANSVERSALES.md: nunca pedirle a un humano que exporte un
secreto a mano).
"""

import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_ENV_PATH = _ROOT / ".env"


def load_dotenv_if_missing() -> None:
    if not _ENV_PATH.exists():
        return
    for line in _ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if key and key not in os.environ:
            os.environ[key] = value

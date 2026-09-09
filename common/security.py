"""
security.py - Autenticacion basica de las interfaces (seccion 19:
"Proteger las interfaces con autenticacion", variables APP_USERNAME /
APP_PASSWORD). Compartida por los tres microservicios.

HTTP Basic, comparacion con secrets.compare_digest (evita timing
attacks). Se aplica como dependencia a nivel de router en cada
servicio - el health check queda exento a proposito (debe responder
sin credenciales para que la plataforma de despliegue pueda
monitorearlo, sin exponer secretos ni excepciones internas).
"""

import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from common.config import get_settings

_security = HTTPBasic()


def require_auth(credentials: HTTPBasicCredentials = Depends(_security)) -> str:
    settings = get_settings()

    usuario_ok = secrets.compare_digest(credentials.username, settings.app_username)
    password_ok = secrets.compare_digest(credentials.password, settings.app_password)

    if not (usuario_ok and password_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

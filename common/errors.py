"""Excepciones de dominio, mapeadas a codigos HTTP en las rutas de los
tres microservicios.

seccion 8: "Usar 400 o 422 para validacion y 409 para conflictos de
relaciones o estado."
"""


class DomainError(Exception):
    """Base de las excepciones de negocio (no de infraestructura)."""


class NotFoundError(DomainError):
    """El recurso solicitado no existe (404)."""


class ValidationDomainError(DomainError):
    """Regla de negocio violada, distinta de validacion de forma (422)."""


class ConflictError(DomainError):
    """Conflicto de relaciones o de estado (409)."""

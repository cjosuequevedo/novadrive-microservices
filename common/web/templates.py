"""
templates.py - Fabrica de Jinja2Templates compartida por los tres
microservicios.

Decision de diseno #4 (CLAUDE.md): en vez de copiar base.html/CSS a
cada servicio, Jinja2Templates busca en DOS carpetas en orden - la de
`common/web/` (donde vive `base.html`, compartido) y la propia del
servicio (donde vive su plantilla especifica, ej. `customers.html`).
`{% extends "base.html" %}` resuelve siempre contra la copia unica de
`common/web/`, sin duplicacion de archivos entre los tres servicios.

Los links de navegacion (`nav.customers_url`, etc.) se inyectan como
globals de Jinja2 una sola vez aca, tomados de Settings - asi
`base.html` los referencia sin que cada ruta tenga que pasarlos en
cada `TemplateResponse`.
"""

from pathlib import Path

from fastapi.templating import Jinja2Templates

from common.config import get_settings

_COMMON_WEB_DIR = Path(__file__).resolve().parent


def build_templates(service_templates_dir: Path) -> Jinja2Templates:
    settings = get_settings()
    templates = Jinja2Templates(directory=[str(_COMMON_WEB_DIR), str(service_templates_dir)])
    templates.env.globals["nav"] = {
        "customers_url": settings.customers_url,
        "inventory_url": settings.inventory_url,
        "orders_url": settings.orders_url,
    }
    return templates

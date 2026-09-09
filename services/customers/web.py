"""Instancia Jinja2Templates de este microservicio - ver
common/web/templates.py para la explicacion de por que busca en dos
carpetas (base.html compartido + customers.html propio)."""

from pathlib import Path

from common.web.templates import build_templates

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

templates = build_templates(_TEMPLATES_DIR)

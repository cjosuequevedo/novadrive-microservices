"""Instancia Jinja2Templates de este microservicio - ver
common/web/templates.py."""

from pathlib import Path

from common.web.templates import build_templates

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

templates = build_templates(_TEMPLATES_DIR)

"""main.py - Aplicacion FastAPI del microservicio Inventory (NovaDrive)."""

from fastapi import FastAPI

from common.web.routes_events import router as events_router
from services.inventory.routes import router as inventory_router

app = FastAPI(title="NovaDrive Inventory")


@app.get("/health")
def health() -> dict:
    """Sin secretos ni excepciones internas (seccion 8). Exento de
    autenticacion a proposito."""
    return {"status": "ok", "service": "inventory"}


app.include_router(inventory_router)
app.include_router(events_router)

"""main.py - Aplicacion FastAPI del microservicio Orders (NovaDrive)."""

from fastapi import FastAPI

from common.web.routes_events import router as events_router
from services.orders.routes import router as orders_router

app = FastAPI(title="NovaDrive Orders")


@app.get("/health")
def health() -> dict:
    """Sin secretos ni excepciones internas (seccion 8). Exento de
    autenticacion a proposito."""
    return {"status": "ok", "service": "orders"}


app.include_router(orders_router)
app.include_router(events_router)

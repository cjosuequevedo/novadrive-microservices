"""main.py - Aplicacion FastAPI del microservicio Customer (NovaDrive)."""

from fastapi import FastAPI

from common.web.routes_events import router as events_router
from services.customers.routes import router as customers_router

app = FastAPI(title="NovaDrive Customers")


@app.get("/health")
def health() -> dict:
    """Sin secretos ni excepciones internas (seccion 8). Exento de
    autenticacion a proposito, para que la plataforma de despliegue
    pueda monitorearlo."""
    return {"status": "ok", "service": "customers"}


app.include_router(customers_router)
app.include_router(events_router)

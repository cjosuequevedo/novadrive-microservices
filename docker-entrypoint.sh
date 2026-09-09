#!/bin/sh
# docker-entrypoint.sh - una sola imagen, cinco entrypoints (decision
# de diseno de CLAUDE.md: "Dockerfile: imagen comun, entrypoint
# seleccionable"). El primer argumento del contenedor decide que pieza
# de NovaDrive corre - mismo espiritu que Andes ("una imagen, dos
# entrypoints"), extendido a los 5 procesos de este proyecto.
set -e

SERVICE="$1"

case "$SERVICE" in
  customers)
    exec uvicorn services.customers.main:app --host 0.0.0.0 --port "${PORT:-8001}"
    ;;
  inventory)
    exec uvicorn services.inventory.main:app --host 0.0.0.0 --port "${PORT:-8002}"
    ;;
  orders)
    exec uvicorn services.orders.main:app --host 0.0.0.0 --port "${PORT:-8003}"
    ;;
  worker)
    exec python -m worker.outbox_worker
    ;;
  bridge)
    export BRIDGE_MODE=loop
    exec python -m bridge.bridge_redpanda_to_databricks
    ;;
  *)
    echo "Unknown service '$SERVICE' - expected one of: customers, inventory, orders, worker, bridge" >&2
    exit 1
    ;;
esac

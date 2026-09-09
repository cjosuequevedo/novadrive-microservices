"""
create_topics.py - Crea los 3 topicos de NovaDrive (seccion 12 del PDF)
en el Redpanda del stack. Idempotente (si ya existen, no falla).

Uso: python -m scripts.create_topics
Requiere KAFKA_BOOTSTRAP_SERVERS en el entorno o en .env (por defecto
apunta al puerto externo del Redpanda de docker-compose.yml, pensado
para correrse desde el host, no desde dentro de un contenedor).
"""

import os

from confluent_kafka.admin import AdminClient, NewTopic

TOPICS = ["cdc.novadrive.customer", "cdc.novadrive.vehicle", "cdc.novadrive.sale"]


def _load_dotenv_if_missing() -> None:
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            if key.strip() and key.strip() not in os.environ:
                os.environ[key.strip()] = value.strip()


def main() -> None:
    _load_dotenv_if_missing()
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092")
    admin = AdminClient({"bootstrap.servers": bootstrap})

    futures = admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in TOPICS])
    for topic, future in futures.items():
        try:
            future.result()
            print(f"OK topic created: {topic}")
        except Exception as exc:  # noqa: BLE001
            if "already exists" in str(exc).lower():
                print(f"= topic already exists: {topic}")
            else:
                print(f"ERROR creating {topic}: {exc}")


if __name__ == "__main__":
    main()

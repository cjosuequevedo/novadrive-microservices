# Imagen unica para los 5 procesos de NovaDrive (customers, inventory,
# orders, worker, bridge) - ver docker-entrypoint.sh. Sin build-essential/
# librdkafka-dev: el despliegue objetivo es x86_64, donde confluent-kafka
# instala desde wheel prebuilt (verificado en esta misma sesion de
# desarrollo, misma leccion que Andes documento para su propio Dockerfile
# - ver LECCIONES_TRANSVERSALES.md seccion 4). Si el despliegue real
# terminara siendo ARM, revisar esa leccion antes de asumir que sigue
# aplicando sin cambios.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ common/
COPY services/ services/
COPY worker/ worker/
COPY bridge/ bridge/
COPY scripts/ scripts/
COPY docker-entrypoint.sh .

RUN chmod +x docker-entrypoint.sh

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["./docker-entrypoint.sh"]
CMD ["customers"]

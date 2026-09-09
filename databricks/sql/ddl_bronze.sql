-- NovaDrive - catalogo propio, SOLO schema bronze (ver CLAUDE.md,
-- decision de diseno #7: sin silver/gold/quarantine, a proposito - eso
-- no es responsabilidad de este proyecto).
CREATE CATALOG IF NOT EXISTS novadrive_catalog;
CREATE SCHEMA IF NOT EXISTS novadrive_catalog.bronze;

-- BRONZE: append-only, evento crudo + metadatos Kafka (seccion 14 del PDF).
-- Mismas 7 columnas que Andes (andes_catalog.bronze.eventos) para que el
-- contrato de Bronze sea identico entre ambos sistemas, aunque cada uno
-- viva en su propio catalogo.
CREATE TABLE IF NOT EXISTS novadrive_catalog.bronze.events (
    kafka_topic     STRING,
    kafka_partition INT,
    kafka_offset    BIGINT,
    kafka_timestamp TIMESTAMP,
    ingested_at     TIMESTAMP,
    payload         STRING,
    source_file     STRING
) USING DELTA;

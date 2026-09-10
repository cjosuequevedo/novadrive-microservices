-- ddl_silver.sql - Silver + Quarantine para NovaDrive (mismo patron que
-- databricks/sql/ddl_medallion.sql de Andes, en archivo separado porque
-- NovaDrive ya separa Bronze en su propio ddl_bronze.sql).
--
-- Cambio de alcance explicito (10 sep 2026): Silver/Quarantine para
-- NovaDrive estaban fuera del alcance original de este proyecto (ver
-- CLAUDE.md, "Alcance recortado", acordado 9 sep 2026 por quien encargo
-- el proyecto y el lider de equipo). CJ confirmo explicitamente que esto
-- es ahora necesario para el trabajo del companero encargado de la
-- arquitectura medallon, quien va a ejecutar este DDL y el notebook
-- manualmente - no se aplico este DDL contra el workspace real en el
-- momento de escribirlo, es un entregable (ver notebook
-- 02b_silver_merge_por_tipo.py para el detalle completo).
--
-- Nombres de tabla = valor de "entity" del contrato de eventos
-- (customer/vehicle/sale) sin traducir - NovaDrive no tenia una
-- convencion de Silver preexistente con la que calzar (a diferencia de
-- Andes, donde silver.clientes/vehiculos/ventas ya existian antes de
-- este pipeline).

CREATE SCHEMA IF NOT EXISTS novadrive_catalog.silver;
CREATE SCHEMA IF NOT EXISTS novadrive_catalog.quarantine;

-- ------------------------------------------------------------------------------
-- SILVER: canonico, MERGE idempotente, is_deleted, stubs por integridad
-- referencial diferida (ver notebook para el detalle del patron de stubs)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS novadrive_catalog.silver.customer (
    customer_key      STRING,   -- source_system + '|' + CUSTOMER_CODE
    source_system     STRING,
    source_record_id  STRING,   -- CUSTOMER_CODE
    document_number   STRING,
    full_name         STRING,
    email_address     STRING,
    mobile_phone      STRING,
    mailing_address   STRING,
    birth_date        DATE,
    status            STRING,   -- ACTIVE | SUSPENDED | CLOSED (homologado de A/S/X)
    is_deleted        BOOLEAN,
    deleted_at        TIMESTAMP,
    occurred_at       TIMESTAMP,
    event_id          STRING,
    updated_at        TIMESTAMP,
    pendiente_completar BOOLEAN  -- true = fila creada como stub por una sale que llego antes que este customer
) USING DELTA;

CREATE TABLE IF NOT EXISTS novadrive_catalog.silver.vehicle (
    vehicle_key       STRING,   -- source_system + '|' + CHASSIS_NUMBER
    source_system     STRING,
    source_record_id  STRING,   -- CHASSIS_NUMBER
    vin               STRING,   -- validado (17 caracteres, sin I/O/Q)
    branch_code       STRING,
    brand             STRING,
    model             STRING,
    model_year        INT,
    exterior_colour   STRING,
    list_amount       DECIMAL(15,2),
    availability      STRING,   -- AVAILABLE | HELD | SOLD (homologado de AVL/HOLD/SOLD)
    is_deleted        BOOLEAN,
    deleted_at        TIMESTAMP,
    occurred_at       TIMESTAMP,
    event_id          STRING,
    updated_at        TIMESTAMP,
    pendiente_completar BOOLEAN  -- true = fila creada como stub por una sale que llego antes que este vehicle
) USING DELTA;

CREATE TABLE IF NOT EXISTS novadrive_catalog.silver.sale (
    sale_key          STRING,   -- source_system + '|' + ORDER_NUMBER
    source_system     STRING,
    source_record_id  STRING,   -- ORDER_NUMBER
    customer_key      STRING,   -- FK logica a silver.customer.customer_key (puede ser un stub)
    vehicle_key       STRING,   -- FK logica a silver.vehicle.vehicle_key (puede ser un stub)
    sales_agent_code  STRING,
    branch_code       STRING,   -- FULFILLMENT_BRANCH
    ordered_at        TIMESTAMP,
    gross_amount      DECIMAL(16,2),
    discount_pct      DECIMAL(7,4),
    tax_amount        DECIMAL(16,2),
    net_amount        DECIMAL(16,2),
    currency_code     CHAR(3),
    status            STRING,   -- OPEN | APPROVED | INVOICED | CANCELLED (ya canonico, sin traducir)
    invoice_reference STRING,
    is_deleted        BOOLEAN,
    deleted_at        TIMESTAMP,
    occurred_at       TIMESTAMP,
    event_id          STRING,
    updated_at        TIMESTAMP
    -- sin pendiente_completar: ninguna otra entidad referencia una sale
    -- como FK en este dominio (mismo criterio que silver.ventas en Andes).
) USING DELTA;

-- ------------------------------------------------------------------------------
-- QUARANTINE: eventos invalidos de las 3 entidades, un solo inbox de
-- triage compartido (mismo criterio que Andes) - append-only, nunca
-- detiene el resto del micro-batch.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS novadrive_catalog.quarantine.events (
    event_id      STRING,
    entity        STRING,
    source_table  STRING,
    operation     STRING,
    payload       STRING,
    reason        STRING,
    created_at    TIMESTAMP
) USING DELTA;

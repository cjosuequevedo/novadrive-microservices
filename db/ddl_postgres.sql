-- NovaDrive (Sistema 2 de AutoNova) — esquema transaccional Postgres
-- Nombres canonicos del PDF (seccion 7): ND_BRANCH, ND_CUSTOMER, ND_SALES_AGENT,
-- ND_INVENTORY_UNIT, ND_SALES_ORDER, ND_ORDER_ITEM, ND_PAYMENT_TXN, ND_OUTBOX_EVENT.
-- Identificadores fisicos en minuscula (decision de diseno #9 en CLAUDE.md): Postgres
-- pliega a minuscula sin comillas, forzar mayuscula obligaria a citar en cada query/ORM.
-- Ejecutado con un usuario de aplicacion (novadrive_app), nunca con un rol superusuario.

CREATE SCHEMA IF NOT EXISTS nd AUTHORIZATION CURRENT_USER;

SET search_path TO nd;

-- ============================================================================
-- ND_BRANCH — catalogo de sedes (dato de referencia, carga sintetica inicial)
-- ============================================================================
CREATE TABLE nd.branch (
    branch_code       VARCHAR(8)   NOT NULL,
    display_name      VARCHAR(120) NOT NULL,
    city_name         VARCHAR(80)  NOT NULL,
    street_address    VARCHAR(240),
    enabled_flag      CHAR(1)      NOT NULL DEFAULT 'Y',
    created_on        TIMESTAMP(6) NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    last_changed_on   TIMESTAMP(6) NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    CONSTRAINT pk_branch PRIMARY KEY (branch_code),
    CONSTRAINT ck_branch_enabled_flag CHECK (enabled_flag IN ('Y', 'N'))
);

-- ============================================================================
-- ND_CUSTOMER — personas o empresas compradoras
-- ============================================================================
CREATE TABLE nd.customer (
    customer_code     VARCHAR(16)  NOT NULL,
    document_number   VARCHAR(25)  NOT NULL,
    full_name         VARCHAR(180) NOT NULL,
    email_address     VARCHAR(180),
    mobile_phone      VARCHAR(35),
    mailing_address   VARCHAR(240),
    birth_date        DATE,
    customer_status   CHAR(1)      NOT NULL DEFAULT 'A',
    created_on        TIMESTAMP(6) NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    last_changed_on   TIMESTAMP(6) NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    CONSTRAINT pk_customer PRIMARY KEY (customer_code),
    CONSTRAINT uq_customer_document UNIQUE (document_number),
    CONSTRAINT ck_customer_status CHECK (customer_status IN ('A', 'S', 'X'))
);

-- ============================================================================
-- ND_SALES_AGENT — asesores comerciales de una sede (dato de referencia)
-- ============================================================================
CREATE TABLE nd.sales_agent (
    agent_code          VARCHAR(12)  NOT NULL,
    branch_code         VARCHAR(8)   NOT NULL,
    agent_display_name  VARCHAR(160) NOT NULL,
    corporate_email     VARCHAR(180),
    active_flag         SMALLINT     NOT NULL DEFAULT 1,
    hired_on            DATE,
    created_on          TIMESTAMP(6) NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    last_changed_on     TIMESTAMP(6) NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    CONSTRAINT pk_sales_agent PRIMARY KEY (agent_code),
    CONSTRAINT fk_sales_agent_branch FOREIGN KEY (branch_code)
        REFERENCES nd.branch (branch_code),
    CONSTRAINT ck_sales_agent_active_flag CHECK (active_flag IN (0, 1))
);

CREATE INDEX ix_sales_agent_branch_code ON nd.sales_agent (branch_code);

-- ============================================================================
-- ND_INVENTORY_UNIT — inventario unitario por numero de chasis
-- ============================================================================
CREATE TABLE nd.inventory_unit (
    chassis_number     VARCHAR(17)   NOT NULL,
    branch_code        VARCHAR(8)    NOT NULL,
    brand_name         VARCHAR(60)   NOT NULL,
    model_name         VARCHAR(100)  NOT NULL,
    model_year         SMALLINT      NOT NULL,
    exterior_colour    VARCHAR(50),
    list_amount        NUMERIC(15,2) NOT NULL,
    availability_code  VARCHAR(10)   NOT NULL DEFAULT 'AVL',
    received_at        TIMESTAMP(6)  NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    created_on         TIMESTAMP(6)  NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    last_changed_on    TIMESTAMP(6)  NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    CONSTRAINT pk_inventory_unit PRIMARY KEY (chassis_number),
    CONSTRAINT fk_inventory_unit_branch FOREIGN KEY (branch_code)
        REFERENCES nd.branch (branch_code),
    CONSTRAINT ck_inventory_unit_list_amount CHECK (list_amount >= 0),
    CONSTRAINT ck_inventory_unit_availability CHECK (availability_code IN ('AVL', 'HOLD', 'SOLD')),
    CONSTRAINT ck_inventory_unit_model_year CHECK (model_year BETWEEN 1900 AND 2100)
);

CREATE INDEX ix_inventory_unit_branch_code ON nd.inventory_unit (branch_code);
CREATE INDEX ix_inventory_unit_availability ON nd.inventory_unit (availability_code);

-- ============================================================================
-- ND_SALES_ORDER — cabecera comercial de una orden
-- ============================================================================
CREATE TABLE nd.sales_order (
    order_number         VARCHAR(24)   NOT NULL,
    buyer_code            VARCHAR(16)   NOT NULL,
    sales_agent_code      VARCHAR(12)   NOT NULL,
    fulfillment_branch    VARCHAR(8)    NOT NULL,
    ordered_at            TIMESTAMP(6)  NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    gross_amount          NUMERIC(16,2) NOT NULL,
    discount_pct          NUMERIC(7,4)  NOT NULL DEFAULT 0,
    tax_amount            NUMERIC(16,2) NOT NULL DEFAULT 0,
    net_amount            NUMERIC(16,2) NOT NULL,
    currency_code         CHAR(3)       NOT NULL DEFAULT 'USD',
    order_status          VARCHAR(10)   NOT NULL DEFAULT 'OPEN',
    invoice_reference     VARCHAR(35),
    created_on            TIMESTAMP(6)  NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    last_changed_on       TIMESTAMP(6)  NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    CONSTRAINT pk_sales_order PRIMARY KEY (order_number),
    CONSTRAINT fk_sales_order_customer FOREIGN KEY (buyer_code)
        REFERENCES nd.customer (customer_code),
    CONSTRAINT fk_sales_order_agent FOREIGN KEY (sales_agent_code)
        REFERENCES nd.sales_agent (agent_code),
    CONSTRAINT fk_sales_order_branch FOREIGN KEY (fulfillment_branch)
        REFERENCES nd.branch (branch_code),
    CONSTRAINT ck_sales_order_gross_amount CHECK (gross_amount >= 0),
    CONSTRAINT ck_sales_order_discount_pct CHECK (discount_pct BETWEEN 0 AND 100),
    CONSTRAINT ck_sales_order_tax_amount CHECK (tax_amount >= 0),
    CONSTRAINT ck_sales_order_net_amount CHECK (net_amount >= 0),
    CONSTRAINT ck_sales_order_status CHECK (order_status IN ('OPEN', 'APPROVED', 'INVOICED', 'CANCELLED'))
);

CREATE INDEX ix_sales_order_buyer_code ON nd.sales_order (buyer_code);
CREATE INDEX ix_sales_order_agent_code ON nd.sales_order (sales_agent_code);
CREATE INDEX ix_sales_order_branch ON nd.sales_order (fulfillment_branch);
CREATE INDEX ix_sales_order_status ON nd.sales_order (order_status);

-- ============================================================================
-- ND_ORDER_ITEM — lineas vehiculo<->orden (sin formulario ni topico propio)
-- ============================================================================
CREATE TABLE nd.order_item (
    order_number         VARCHAR(24)   NOT NULL,
    line_number           SMALLINT      NOT NULL,
    chassis_number         VARCHAR(17)   NOT NULL,
    unit_gross_amount      NUMERIC(16,2) NOT NULL,
    line_discount_pct      NUMERIC(7,4)  NOT NULL DEFAULT 0,
    unit_net_amount        NUMERIC(16,2) NOT NULL,
    created_on             TIMESTAMP(6)  NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    last_changed_on        TIMESTAMP(6)  NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    CONSTRAINT pk_order_item PRIMARY KEY (order_number, line_number),
    CONSTRAINT fk_order_item_order FOREIGN KEY (order_number)
        REFERENCES nd.sales_order (order_number),
    CONSTRAINT fk_order_item_unit FOREIGN KEY (chassis_number)
        REFERENCES nd.inventory_unit (chassis_number),
    CONSTRAINT uq_order_item_chassis UNIQUE (chassis_number),
    CONSTRAINT ck_order_item_unit_gross CHECK (unit_gross_amount >= 0),
    CONSTRAINT ck_order_item_discount_pct CHECK (line_discount_pct BETWEEN 0 AND 100),
    CONSTRAINT ck_order_item_unit_net CHECK (unit_net_amount >= 0)
);

-- ============================================================================
-- ND_PAYMENT_TXN — transacciones de cobro (sin topico propio en el MVP)
-- ============================================================================
CREATE TABLE nd.payment_txn (
    payment_reference   VARCHAR(30)   NOT NULL,
    order_number         VARCHAR(24)   NOT NULL,
    payment_channel      VARCHAR(12)   NOT NULL,
    captured_amount      NUMERIC(16,2) NOT NULL,
    currency_code        CHAR(3)       NOT NULL DEFAULT 'USD',
    payment_status        VARCHAR(10)   NOT NULL DEFAULT 'PENDING',
    captured_at           TIMESTAMP(6),
    provider_code         VARCHAR(30),
    created_on             TIMESTAMP(6)  NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    last_changed_on        TIMESTAMP(6)  NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    CONSTRAINT pk_payment_txn PRIMARY KEY (payment_reference),
    CONSTRAINT fk_payment_txn_order FOREIGN KEY (order_number)
        REFERENCES nd.sales_order (order_number),
    CONSTRAINT ck_payment_txn_amount CHECK (captured_amount >= 0)
);

CREATE INDEX ix_payment_txn_order_number ON nd.payment_txn (order_number);

-- ============================================================================
-- ND_OUTBOX_EVENT — cola tecnica durable (sin FKs, sobrevive al borrado del origen)
-- ============================================================================
CREATE TABLE nd.outbox_event (
    outbox_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    event_id           VARCHAR(36)   NOT NULL,
    event_version      SMALLINT      NOT NULL DEFAULT 1,
    source_system      VARCHAR(20)   NOT NULL,
    entity_name        VARCHAR(30)   NOT NULL,
    source_table       VARCHAR(40)   NOT NULL,
    operation          CHAR(1)       NOT NULL,
    record_key_json    TEXT          NOT NULL,
    kafka_topic        VARCHAR(180)  NOT NULL,
    kafka_key          VARCHAR(300)  NOT NULL,
    payload_json       TEXT          NOT NULL,
    status             VARCHAR(15)   NOT NULL DEFAULT 'PENDING',
    attempts           INTEGER       NOT NULL DEFAULT 0,
    next_retry_at      TIMESTAMPTZ(6) NOT NULL,
    occurred_at        TIMESTAMPTZ(6) NOT NULL,
    created_at         TIMESTAMPTZ(6) NOT NULL,
    claimed_at         TIMESTAMPTZ(6),
    published_at       TIMESTAMPTZ(6),
    last_error         VARCHAR(1000),
    correlation_id     VARCHAR(36)   NOT NULL,
    CONSTRAINT pk_outbox_event PRIMARY KEY (outbox_id),
    CONSTRAINT uq_outbox_event_id UNIQUE (event_id),
    CONSTRAINT ck_outbox_operation CHECK (operation IN ('c', 'u', 'd')),
    CONSTRAINT ck_outbox_status CHECK (status IN ('PENDING', 'PROCESSING', 'RETRY', 'PUBLISHED', 'FAILED')),
    CONSTRAINT ck_outbox_entity CHECK (entity_name IN ('customer', 'vehicle', 'sale'))
);

-- Indice obligatorio (seccion 6/13 del PDF) para el reclamo de lote del worker.
CREATE INDEX ix_outbox_status_retry_created
    ON nd.outbox_event (status, next_retry_at, created_at);

RESET search_path;

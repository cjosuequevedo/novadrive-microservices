-- ddl_gold.sql - Gold para NovaDrive: vistas SQL puras sobre Silver, sin
-- notebook ni streaming (mismo criterio que Gold en Andes - agregaciones
-- sobre datos ya resueltos no necesitan Auto Loader ni Job de Databricks,
-- serian computo desperdiciado). `CREATE OR REPLACE VIEW` no consume
-- computo sostenido, solo al momento de aplicarlo y cada vez que alguien
-- consulta la vista despues.
--
-- Cambio de alcance explicito (10 sep 2026): Gold para NovaDrive tambien
-- estaba fuera del alcance original (ver CLAUDE.md, "## Alcance
-- recortado") - CJ confirmo explicitamente que ahora si es necesario,
-- misma decision que ya se tomo para Silver/Quarantine.
--
-- 2 limitaciones reales, confirmadas con evidencia (10 sep 2026, no
-- asumidas):
--   1. Las vistas de inventario/catalogo filtran pendiente_completar =
--      FALSE (excluye stubs sin resolver) - mismo criterio que Andes.
--   2. branch_code/sales_agent_code quedan como CODIGOS CRUDOS, no
--      nombres legibles - confirmado con GET real a la Unity Catalog
--      REST API que novadrive_catalog.bronze.branch/sales_agent NO
--      existen (ND_BRANCH/ND_SALES_AGENT nunca tuvieron topico propio,
--      ver CLAUDE.md seccion "Modelo relacional Postgres" - dato de
--      referencia sembrado directo en Postgres, nunca pasa por el
--      outbox/Kafka/Bronze). Mismo comportamiento que Andes.

CREATE SCHEMA IF NOT EXISTS novadrive_catalog.gold;

-- Resumen de ventas por sucursal+estado. discount_pct es un PORCENTAJE
-- (no un monto, a diferencia de Andes) - por eso se promedia, no se suma.
CREATE OR REPLACE VIEW novadrive_catalog.gold.sales_summary AS
SELECT
    branch_code,
    status,
    COUNT(*)                        AS total_sales,
    ROUND(SUM(gross_amount), 2)     AS gross_amount_total,
    ROUND(AVG(discount_pct), 4)     AS avg_discount_pct,
    ROUND(SUM(net_amount), 2)       AS net_amount_total,
    ROUND(AVG(net_amount), 2)       AS avg_ticket
FROM novadrive_catalog.silver.sale
WHERE is_deleted = FALSE
GROUP BY branch_code, status;

-- Tendencia diaria (para grafico de linea en un dashboard).
CREATE OR REPLACE VIEW novadrive_catalog.gold.sales_by_day AS
SELECT
    DATE(COALESCE(ordered_at, occurred_at)) AS sale_date,
    branch_code,
    COUNT(*)                                AS total_sales,
    ROUND(SUM(net_amount), 2)               AS net_amount_total,
    ROUND(AVG(net_amount), 2)               AS avg_ticket
FROM novadrive_catalog.silver.sale
WHERE is_deleted = FALSE
GROUP BY DATE(COALESCE(ordered_at, occurred_at)), branch_code;

-- Inventario disponible - excluye stubs (pendiente_completar) ademas
-- de borrados, mismo motivo que en Andes.
CREATE OR REPLACE VIEW novadrive_catalog.gold.available_inventory AS
SELECT
    brand,
    model,
    availability,
    COUNT(*)                       AS units,
    ROUND(AVG(list_amount), 2)     AS avg_list_amount
FROM novadrive_catalog.silver.vehicle
WHERE is_deleted = FALSE
  AND pendiente_completar = FALSE
GROUP BY brand, model, availability;

-- Que marca/modelo vende mas e ingresa mas (join real sale<->vehicle).
CREATE OR REPLACE VIEW novadrive_catalog.gold.top_selling_models AS
SELECT
    v.brand,
    v.model,
    COUNT(*)                          AS units_sold,
    ROUND(SUM(s.net_amount), 2)       AS total_revenue,
    ROUND(AVG(s.net_amount), 2)       AS avg_sale_amount
FROM novadrive_catalog.silver.sale s
JOIN novadrive_catalog.silver.vehicle v ON v.vehicle_key = s.vehicle_key
WHERE s.is_deleted = FALSE
  AND v.pendiente_completar = FALSE
GROUP BY v.brand, v.model
ORDER BY units_sold DESC;

-- Resumen de clientes. OJO: status tiene 3 valores (ACTIVE/SUSPENDED/
-- CLOSED), no 2 como Andes (ACTIVE/INACTIVE) - la vista lo soporta sola
-- porque agrupa por status tal cual viene.
CREATE OR REPLACE VIEW novadrive_catalog.gold.customers_summary AS
SELECT
    status,
    COUNT(*)                                             AS total_customers,
    SUM(CASE WHEN pendiente_completar THEN 1 ELSE 0 END) AS customers_pending_sync
FROM novadrive_catalog.silver.customer
WHERE is_deleted = FALSE
GROUP BY status;

-- Consolidada fila por fila (no agregada) - cada venta con el nombre
-- real del cliente y marca/modelo del vehiculo, no las claves crudas.
-- LEFT JOIN a proposito: una venta con un stub sin resolver todavia
-- debe seguir apareciendo, no desaparecer de la vista.
CREATE OR REPLACE VIEW novadrive_catalog.gold.sales_detail AS
SELECT
    s.sale_key,
    s.invoice_reference,
    s.status,
    s.branch_code,
    s.sales_agent_code,
    c.full_name  AS customer_name,
    v.brand      AS vehicle_brand,
    v.model      AS vehicle_model,
    s.gross_amount,
    s.discount_pct,
    s.net_amount,
    s.ordered_at
FROM novadrive_catalog.silver.sale s
LEFT JOIN novadrive_catalog.silver.customer c ON c.customer_key = s.customer_key
LEFT JOIN novadrive_catalog.silver.vehicle v ON v.vehicle_key = s.vehicle_key
WHERE s.is_deleted = FALSE;

-- NO es una metrica de negocio, es operativa (salud del pipeline) -
-- separada aparte a proposito. Cuenta stubs de customer/vehicle sin
-- resolver - si crece sin bajar nunca, algo rio arriba dejo de publicar
-- esos eventos.
CREATE OR REPLACE VIEW novadrive_catalog.gold.pipeline_health AS
SELECT 'customer' AS entity, COUNT(*) AS pending_sync
FROM novadrive_catalog.silver.customer WHERE pendiente_completar = TRUE
UNION ALL
SELECT 'vehicle', COUNT(*)
FROM novadrive_catalog.silver.vehicle WHERE pendiente_completar = TRUE;

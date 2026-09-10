# NovaDrive — Sistema transaccional (Sistema 2 de AutoNova)

Conjunto de microservicios Python/FastAPI que registran customers, inventory units y sales orders de NovaDrive en Postgres mediante el patrón transactional outbox, con un worker que publica cada cambio a Redpanda y un bridge que sube esos eventos crudos a Databricks **Bronze únicamente**. Es el hermano de **Andes Motors** (Sistema 1 — Oracle, monolito, español, ya completo y desplegado, repo separado `andes-motors`) — comparten el mismo contrato de eventos, pero **no comparten base de datos, ni Redpanda, ni el resto del pipeline analítico**.

> Fuente normativa: `Actualización del entregable..pdf` (v1.0, 7 sep 2026), secciones 7, 8, 9, 11, 12 y 19 — más el recorte de alcance y los estándares de interfaz acordados el 9 sep 2026 (ver `## Alcance recortado`, que ANULA lo que el documento sugiera sobre Silver/Gold/Quarantine para este proyecto). Ante cualquier ambigüedad no cubierta por el recorte, el PDF manda sobre este CLAUDE.md.

## ⚠️ Punto de partida obligatorio para una sesión nueva

Este documento es la única fuente de verdad de las decisiones tomadas hasta ahora. **Antes de tocar código, leer completo `## Alcance recortado`, `## Decisiones de diseño no especificadas por el documento`, `## Estado exacto de avance` y `## Pendientes explícitos` (en ese orden, y respetar el orden interno de los pendientes — no saltar al punto 2 sin que CJ haya aprobado el punto 1).** Verificar también el estado real de Docker (`docker ps`) antes de asumir si la pila está arriba o abajo — no inferirlo de este documento, ver la nota de la sesión de pruebas manuales en `## Estado exacto de avance`. Si en algún momento el trabajo empieza a acercarse a Silver/Gold/Quarantine/homologación, **PARAR y preguntar** — es señal de que algo se malinterpretó (ver razón en la sección de alcance).

## Alcance recortado (confirmado 9 sep 2026 con quien encargó el proyecto y el líder de equipo)

> **⚠️ AMPLIACIÓN DE ALCANCE — YA CUBRE TODA LA ARQUITECTURA MEDALLÓN (10 sep 2026) — leer antes de asumir que "NO es responsabilidad de este proyecto" sigue vigente tal cual.** CJ confirmó explícitamente que Silver y Quarantine para NovaDrive ahora SÍ son necesarios (ver Fase 17), para el trabajo del compañero encargado de la arquitectura medallón — que ejecuta todo manualmente, igual que con Bronze. **Actualización del mismo día**: CJ avisó que Gold también se necesitaba, y ese mismo día llegaron las indicaciones concretas — Gold **ya se construyó y se aplicó de verdad contra el workspace** (ver Fase 18, a diferencia de Bronze/Silver que quedaron como entregable sin ejecutar). Esta sesión señaló la contradicción con el acuerdo original antes de proceder con Silver, y CJ confirmó que es una decisión consciente; el aviso de Gold llegó después, sin objeción de esta sesión (mismo criterio ya establecido, no hacía falta repetir la pregunta). **No verificado que el líder de equipo original (co-firmante del recorte del 9 sep) esté al tanto de este cambio de alcance** — si una sesión futura necesita ese contexto, preguntarlo explícitamente, no asumir que el acuerdo original completo sigue intacto. Ver Fase 17 (Silver, entregable sin ejecutar) y Fase 18 (Gold, aplicado real).

Esto **anula** lo que la sección 2 del PDF (tabla "Resultado obligatorio") sugiere como entregable completo de NovaDrive — con la salvedad de Silver/Quarantine señalada arriba.

**SÍ es responsabilidad de este proyecto:**
- Sistema transaccional en **microservicios**, **Postgres**, formularios/API en **inglés**, para Customer, Inventory y Order.
- Patrón transactional outbox: misma tabla técnica `ND_OUTBOX_EVENT`, mismo contrato de eventos que Andes.
- Worker independiente publicando a Redpanda (tópicos `cdc.novadrive.*`).
- Bridge subiendo esos eventos **crudos** a Databricks **Bronze** (append-only). Ahí termina el trabajo con Databricks.

**NO es responsabilidad de este proyecto** (no construirlo aunque el PDF lo liste como parte del entregable de la sección 2/14):
- ~~Silver, Gold ni Quarantine para NovaDrive.~~ **Ya no vigente — ver la nota de ampliación de alcance justo arriba de este encabezado.** Silver/Quarantine construidos como entregable (Fase 17); Gold construido Y aplicado real contra el workspace (Fase 18).
- Homologación entre esquemas Andes/NovaDrive (mapeos de las secciones 15-17 del PDF).
- Dashboards Lakeview ni vistas de negocio derivadas.
- Cualquier decisión de cómo los analistas consumen o transforman lo que llega a Bronze.

**Por qué existe este recorte**: la sesión de Andes Motors invirtió tiempo real construyendo Silver/Gold para su propio sistema sin que fuera estrictamente necesario para la demo; para evitar repetir ese mismo desvío en NovaDrive, ambas personas de autoridad confirmaron explícitamente que el alcance de este proyecto se detiene en Bronze.

**Diferencias de arquitectura respecto a Andes** (decisión explícita, sección 1 del PDF actualizada):
- Dos bases de datos **distintas e independientes** (Oracle para Andes, Postgres para NovaDrive) — ya estaba así.
- Dos **arquitecturas distintas a propósito**: Andes es monolito, NovaDrive es **microservicios** — para demostrar que la solución (outbox + Redpanda + Bronze) es agnóstica de la arquitectura del sistema transaccional que la alimenta.
- Cada proyecto vive en su **propio VPS/despliegue**, por lo tanto **Redpanda también es propio de NovaDrive**, no compartido con Andes (confirmado 9 sep 2026 — más simple de razonar y desplegar de forma independiente; si aparece una razón técnica real para compartirlo, se avisa antes de asumirlo, no se cambia en silencio).
- Databricks: mismo workspace que Andes (no hay indicación de separarlo), pero **catálogo propio** `novadrive_catalog` con **solo el schema `bronze`** — a propósito, para que la separación de responsabilidades (sin Silver/Gold/Quarantine para NovaDrive) quede reforzada también a nivel de infraestructura, no solo de código. No tocar `andes_catalog`.

## Stack obligatorio

| Área | Tecnología | Notas |
|---|---|---|
| Web | Python, FastAPI, Jinja2 | Formularios server-rendered en inglés, rutas POST, health check por servicio |
| Persistencia | SQLAlchemy 2, psycopg (driver Postgres) | Modelos ORM, transacciones explícitas, sin rol ADMIN/superusuario de Postgres |
| Configuración | Pydantic Settings | Variables y secretos por entorno, nunca hardcodeados |
| Mensajería | confluent-kafka | Productor compatible con Redpanda propio de NovaDrive (SASL/SSL) |
| Pruebas | pytest | Servicios, transacciones, eventos y rutas |
| Contenedores | Docker | Una imagen por servicio (o una imagen común con entrypoint distinto — ver estructura) |
| Prohibido | pandas | No usar pandas para transacciones ni para simular la base operativa |

## Modelo relacional Postgres (esquema ND_*)

Convención: tablas y campos en inglés, **claves principales alfanuméricas** (códigos de negocio, no autoincrementales), descuentos como **porcentaje** (no monto, a diferencia de Andes).

### Relaciones (FK)

| Tabla padre | Relación | Tabla hija | Clave foránea |
|---|---|---|---|
| ND_BRANCH | 1 a N | ND_SALES_AGENT | BRANCH_CODE |
| ND_BRANCH | 1 a N | ND_INVENTORY_UNIT | BRANCH_CODE |
| ND_BRANCH | 1 a N | ND_SALES_ORDER | FULFILLMENT_BRANCH |
| ND_CUSTOMER | 1 a N | ND_SALES_ORDER | BUYER_CODE |
| ND_SALES_AGENT | 1 a N | ND_SALES_ORDER | SALES_AGENT_CODE |
| ND_SALES_ORDER | 1 a N | ND_ORDER_ITEM | ORDER_NUMBER |
| ND_INVENTORY_UNIT | 1 a 0 o 1 | ND_ORDER_ITEM | CHASSIS_NUMBER (único) |
| ND_SALES_ORDER | 1 a N | ND_PAYMENT_TXN | ORDER_NUMBER |

### ND_BRANCH — catálogo de sedes (dato de referencia, carga sintética inicial)

| Campo | Tipo Postgres | Uso |
|---|---|---|
| BRANCH_CODE | VARCHAR(8) NOT NULL | **PK** |
| DISPLAY_NAME | VARCHAR(120) NOT NULL | |
| CITY_NAME | VARCHAR(80) NOT NULL | |
| STREET_ADDRESS | VARCHAR(240) | |
| ENABLED_FLAG | CHAR(1) DEFAULT 'Y' NOT NULL | |
| CREATED_ON | TIMESTAMP(6) DEFAULT now() NOT NULL | |
| LAST_CHANGED_ON | TIMESTAMP(6) DEFAULT now() NOT NULL | |

Sin formulario ni tópico propio — se administra vía script de siembra, igual que `AM_SUCURSAL` en Andes.

### ND_CUSTOMER — personas o empresas compradoras

| Campo | Tipo Postgres | Uso |
|---|---|---|
| CUSTOMER_CODE | VARCHAR(16) NOT NULL | **PK**, generado por la app (ver `## Decisiones de diseño`) |
| DOCUMENT_NUMBER | VARCHAR(25) NOT NULL | Único (regla de negocio) |
| FULL_NAME | VARCHAR(180) NOT NULL | Un solo campo, no separado en nombres/apellidos |
| EMAIL_ADDRESS | VARCHAR(180) | |
| MOBILE_PHONE | VARCHAR(35) | |
| MAILING_ADDRESS | VARCHAR(240) | |
| BIRTH_DATE | DATE | |
| CUSTOMER_STATUS | CHAR(1) DEFAULT 'A' NOT NULL | `A` activo, `S` suspendido, `X` cerrado |
| CREATED_ON | TIMESTAMP(6) DEFAULT now() NOT NULL | |
| LAST_CHANGED_ON | TIMESTAMP(6) DEFAULT now() NOT NULL | |

Regla: alta/edición/eliminación por formulario. **No eliminar si ya tiene órdenes.** No permitir `DOCUMENT_NUMBER` duplicado.

### ND_SALES_AGENT — asesores comerciales de una sede (dato de referencia)

| Campo | Tipo Postgres | Uso |
|---|---|---|
| AGENT_CODE | VARCHAR(12) NOT NULL | **PK** |
| BRANCH_CODE | VARCHAR(8) NOT NULL | FK |
| AGENT_DISPLAY_NAME | VARCHAR(160) NOT NULL | |
| CORPORATE_EMAIL | VARCHAR(180) | |
| ACTIVE_FLAG | SMALLINT DEFAULT 1 NOT NULL | |
| HIRED_ON | DATE | |
| CREATED_ON | TIMESTAMP(6) DEFAULT now() NOT NULL | |
| LAST_CHANGED_ON | TIMESTAMP(6) DEFAULT now() NOT NULL | |

Sin formulario ni tópico propio — siembra igual que `AM_VENDEDOR`. Regla: solo un agente **activo**, perteneciente a la sede de entrega de la orden, puede registrar órdenes.

### ND_INVENTORY_UNIT — inventario unitario por número de chasis

| Campo | Tipo Postgres | Uso |
|---|---|---|
| CHASSIS_NUMBER | VARCHAR(17) NOT NULL | **PK**, VIN real ingresado por el usuario |
| BRANCH_CODE | VARCHAR(8) NOT NULL | FK |
| BRAND_NAME | VARCHAR(60) NOT NULL | |
| MODEL_NAME | VARCHAR(100) NOT NULL | |
| MODEL_YEAR | SMALLINT NOT NULL | |
| EXTERIOR_COLOUR | VARCHAR(50) | |
| LIST_AMOUNT | NUMERIC(15,2) NOT NULL | **CHECK >= 0** |
| AVAILABILITY_CODE | VARCHAR(10) DEFAULT 'AVL' NOT NULL | `AVL` / `HOLD` / `SOLD` |
| RECEIVED_AT | TIMESTAMP(6) DEFAULT now() NOT NULL | |
| CREATED_ON | TIMESTAMP(6) DEFAULT now() NOT NULL | |
| LAST_CHANGED_ON | TIMESTAMP(6) DEFAULT now() NOT NULL | |

Regla: alta/edición/eliminación por formulario. **No eliminar si ya pertenece a una orden.** No permitir `CHASSIS_NUMBER` duplicado (es la PK, ya lo garantiza). Aparece como máximo en un `ND_ORDER_ITEM`.

### ND_SALES_ORDER — cabecera comercial de una orden

| Campo | Tipo Postgres | Uso |
|---|---|---|
| ORDER_NUMBER | VARCHAR(24) NOT NULL | **PK**, autogenerado por la app |
| BUYER_CODE | VARCHAR(16) NOT NULL | FK → ND_CUSTOMER |
| SALES_AGENT_CODE | VARCHAR(12) NOT NULL | FK → ND_SALES_AGENT |
| FULFILLMENT_BRANCH | VARCHAR(8) NOT NULL | FK → ND_BRANCH |
| ORDERED_AT | TIMESTAMP(6) DEFAULT now() NOT NULL | |
| GROSS_AMOUNT | NUMERIC(16,2) NOT NULL | **CHECK >= 0** |
| DISCOUNT_PCT | NUMERIC(7,4) DEFAULT 0 NOT NULL | **CHECK entre 0 y 100** (puntos porcentuales, ver decisión) |
| TAX_AMOUNT | NUMERIC(16,2) DEFAULT 0 NOT NULL | **CHECK >= 0** |
| NET_AMOUNT | NUMERIC(16,2) NOT NULL | **CHECK >= 0** |
| CURRENCY_CODE | CHAR(3) DEFAULT 'USD' NOT NULL | |
| ORDER_STATUS | VARCHAR(10) DEFAULT 'OPEN' NOT NULL | Ver máquina de estados |
| INVOICE_REFERENCE | VARCHAR(35) | |
| CREATED_ON | TIMESTAMP(6) DEFAULT now() NOT NULL | |
| LAST_CHANGED_ON | TIMESTAMP(6) DEFAULT now() NOT NULL | |

Regla: el formulario administra cabecera, item, pago opcional y disponibilidad del inventario **en una sola transacción**. Customer, agent, branch e inventory unit deben existir y estar habilitados; el agent debe pertenecer a la `FULFILLMENT_BRANCH`. El item se crea automáticamente.

### ND_ORDER_ITEM — líneas de vehículo (sin formulario ni tópico propio; va dentro del evento `sale`)

| Campo | Tipo Postgres | Uso |
|---|---|---|
| ORDER_NUMBER | VARCHAR(24) NOT NULL | **PK compuesta** (1/2), FK → ND_SALES_ORDER |
| LINE_NUMBER | SMALLINT NOT NULL | **PK compuesta** (2/2) |
| CHASSIS_NUMBER | VARCHAR(17) NOT NULL | FK, único (0 o 1 línea por unidad) |
| UNIT_GROSS_AMOUNT | NUMERIC(16,2) NOT NULL | **CHECK >= 0** |
| LINE_DISCOUNT_PCT | NUMERIC(7,4) DEFAULT 0 NOT NULL | **CHECK entre 0 y 100** |
| UNIT_NET_AMOUNT | NUMERIC(16,2) NOT NULL | **CHECK >= 0** |
| CREATED_ON / LAST_CHANGED_ON | TIMESTAMP(6) NOT NULL | |

### ND_PAYMENT_TXN — transacciones de cobro (sin tópico propio en el MVP; va dentro del evento `sale`)

| Campo | Tipo Postgres | Uso |
|---|---|---|
| PAYMENT_REFERENCE | VARCHAR(30) NOT NULL | **PK** |
| ORDER_NUMBER | VARCHAR(24) NOT NULL | FK |
| PAYMENT_CHANNEL | VARCHAR(12) NOT NULL | |
| CAPTURED_AMOUNT | NUMERIC(16,2) NOT NULL | **CHECK >= 0** |
| CURRENCY_CODE | CHAR(3) DEFAULT 'USD' NOT NULL | |
| PAYMENT_STATUS | VARCHAR(10) DEFAULT 'PENDING' NOT NULL | |
| CAPTURED_AT | TIMESTAMP(6) | |
| PROVIDER_CODE | VARCHAR(30) | |
| CREATED_ON / LAST_CHANGED_ON | TIMESTAMP(6) NOT NULL | |

### ND_OUTBOX_EVENT — cola técnica durable (sin FKs, para sobrevivir al borrado del origen)

| Campo | Tipo Postgres | Uso |
|---|---|---|
| OUTBOX_ID | BIGINT GENERATED ALWAYS AS IDENTITY | **PK**, orden durable |
| EVENT_ID | VARCHAR(36) NOT NULL UNIQUE | UUID, idempotencia |
| EVENT_VERSION | SMALLINT DEFAULT 1 NOT NULL | |
| SOURCE_SYSTEM | VARCHAR(20) NOT NULL | Siempre `NOVADRIVE` en este proyecto |
| ENTITY_NAME | VARCHAR(30) NOT NULL | `customer`, `vehicle` o `sale` |
| SOURCE_TABLE | VARCHAR(40) NOT NULL | Tabla física de origen |
| OPERATION | CHAR(1) NOT NULL | `c`, `u` o `d` |
| RECORD_KEY_JSON | TEXT NOT NULL | Clave primaria serializada (JSONB también válido) |
| KAFKA_TOPIC | VARCHAR(180) NOT NULL | Ver tabla de tópicos |
| KAFKA_KEY | VARCHAR(300) NOT NULL | Clave estable de partición |
| PAYLOAD_JSON | TEXT NOT NULL | Evento completo (contrato abajo) |
| STATUS | VARCHAR(15) DEFAULT 'PENDING' | PENDING, PROCESSING, RETRY, PUBLISHED, FAILED |
| ATTEMPTS | INTEGER DEFAULT 0 NOT NULL | |
| NEXT_RETRY_AT | TIMESTAMPTZ(6) NOT NULL | |
| OCCURRED_AT | TIMESTAMPTZ(6) NOT NULL | Momento del cambio de negocio |
| CREATED_AT | TIMESTAMPTZ(6) NOT NULL | |
| CLAIMED_AT | TIMESTAMPTZ(6) | |
| PUBLISHED_AT | TIMESTAMPTZ(6) | |
| LAST_ERROR | VARCHAR(1000) | |
| CORRELATION_ID | VARCHAR(36) NOT NULL | |

Índice obligatorio sobre `(STATUS, NEXT_RETRY_AT, CREATED_AT)`.

**Nota sobre tipos de fecha**: el PDF define las tablas de negocio `ND_*` con `TIMESTAMP(6) DEFAULT LOCALTIMESTAMP` (sin zona horaria) pero `ND_OUTBOX_EVENT` con `TIMESTAMP(6) WITH TIME ZONE` — es una diferencia real en el propio documento, no un error de transcripción de este archivo. Se traslada tal cual: columnas de negocio → `TIMESTAMP(6)` naive en Postgres; columnas del outbox → `TIMESTAMPTZ(6)`. El valor efectivo que la app escribe en ambos casos se calcula siempre en UTC — la ausencia de zona horaria en las columnas de negocio es solo el tipo de columna, no una licencia para escribir hora local.

## Restricciones de diseño NO NEGOCIABLES (sección 4 del PDF)

- No usar pandas para las transacciones ni para simular la base operativa.
- No ejecutar DML manual durante la demostración; las mutaciones nacen en los formularios o la API.
- No publicar directamente a Redpanda desde el endpoint sin registrar primero el outbox.
- No recargar, truncar ni sobrescribir tablas completas por cada evento.
- No iniciar un Job de Databricks por cada POST; mantener un consumidor continuo (aunque acá "continuo" solo llega hasta Bronze).
- No utilizar un rol superusuario/ADMIN de Postgres desde las aplicaciones.
- No guardar wallets, contraseñas, tokens ni datos personales reales en Git.
- Conservar las diferencias de origen en Bronze — **no aplica** la segunda mitad de esta regla del PDF ("resolverlas en Silver") porque Silver está fuera de alcance; Bronze simplemente conserva el JSON tal cual llega.

Adicional del comportamiento del POST: el endpoint **no** lanza ningún proceso de Databricks ni espera confirmación de Bronze; confirma Postgres + outbox, devuelve `correlation_id`, y deja que worker + bridge completen el recorrido. Meta: < 10 segundos de POST a llegada a Bronze en condiciones normales.

### Comportamiento del POST — reglas explícitas (sección 8)

- **Toda respuesta HTTP de una mutación debe incluir `correlation_id` Y la lista de `event_id` generados** en esa misma transacción. Una orden puede producir `sale` + `vehicle` — la respuesta lista ambos.
- **Debe existir un endpoint de consulta de estado de evento outbox** (ej. `GET /events/{event_id}/status`) devolviendo `STATUS`, `ATTEMPTS`, `NEXT_RETRY_AT`, `PUBLISHED_AT`, `LAST_ERROR`.
- 400/422 para validación, 409 para conflictos de relación o estado. Health check sin secretos ni excepciones internas.

## Rutas HTTP obligatorias (sección 8)

| Entidad | Alta | Edición | Eliminación |
|---|---|---|---|
| Customer | POST /customers | POST /customers/{code}/edit | POST /customers/{code}/delete |
| Inventory | POST /inventory | POST /inventory/{chassis}/edit | POST /inventory/{chassis}/delete |
| Order | POST /orders | POST /orders/{number}/edit | POST /orders/{number}/delete |

## Contrato de eventos (sección 11) — compartido con Andes, sin modificar

```json
{
  "event_id": "uuid",
  "event_version": 1,
  "source_system": "NOVADRIVE",
  "entity": "customer | vehicle | sale",
  "source_table": "ND_CUSTOMER | ND_INVENTORY_UNIT | ND_SALES_ORDER",
  "operation": "c | u | d",
  "record_key": { "...": "clave primaria" },
  "before": null,
  "after": { "...": "snapshot" },
  "occurred_at": "ISO 8601 UTC",
  "correlation_id": "uuid"
}
```

| Operación | before | after | Interpretación |
|---|---|---|---|
| c | null | snapshot nuevo | Creación |
| u | snapshot anterior | snapshot nuevo | Actualización |
| d | snapshot eliminado | null | Eliminación |

- Clave estable de partición: `NOVADRIVE|customer|CUSTOMER_CODE`, `NOVADRIVE|vehicle|CHASSIS_NUMBER`, `NOVADRIVE|sale|ORDER_NUMBER`.
- Importes serializados como `Decimal` (nunca `float`); fechas en UTC.
- **Igual que Andes: los montos van como STRING en `PAYLOAD_JSON`** (ej. `"net_amount": "28000.10"`), nunca como número JSON desnudo — blinda contra reinterpretación como `float` en cualquier lector sin schema explícito, Bronze incluido.
- Una orden puede producir **dos** eventos con el mismo `correlation_id`: `sale` (c/u/d) y `vehicle` (u) por el cambio de disponibilidad.
- La entrega es *at-least-once*. Como no hay Silver en este proyecto, la deduplicación por `event_id` es responsabilidad de quien consuma Bronze después — Bronze en sí es append-only y puede (y debe) tener duplicados en caso de reintento, eso es esperado y correcto.

### Tópicos Redpanda (NovaDrive, broker propio)

| Entidad | Tópico | Clave |
|---|---|---|
| customer | `cdc.novadrive.customer` | `NOVADRIVE\|customer\|CUSTOMER_CODE` |
| vehicle | `cdc.novadrive.vehicle` | `NOVADRIVE\|vehicle\|CHASSIS_NUMBER` |
| sale | `cdc.novadrive.sale` | `NOVADRIVE\|sale\|ORDER_NUMBER` |

Una partición por tópico en el MVP. SASL/SSL, `acks=all`, idempotencia habilitada. No crear tópicos distintos para `c`/`u`/`d`. Broker propio de NovaDrive, no compartido con Andes (ver `## Decisiones de diseño`).

## Reglas transaccionales de negocio (sección 9)

1. Generar `correlation_id` al iniciar cada petición de modificación.
2. Validar el formulario y abrir una transacción Postgres.
3. **Bloquear con `SELECT ... FOR UPDATE`** las filas expuestas a concurrencia, **especialmente `ND_INVENTORY_UNIT`**.
4. Aplicar el cambio mediante SQLAlchemy.
5. Crear los eventos outbox con `before`/`after` dentro de la misma transacción.
6. **Un solo `commit`**; cualquier error revierte negocio y outbox juntos.
7. Devolver `correlation_id` de inmediato; la publicación continúa de forma asíncrona vía el worker.

Reglas específicas:
- **Customer**: no permitir `DOCUMENT_NUMBER` duplicado; no eliminar con órdenes asociadas (409).
- **Inventory**: no permitir `CHASSIS_NUMBER` duplicado (PK); no eliminar unidades ya vinculadas a una orden (409).
- **Order**: customer, agent, branch e inventory unit deben existir y estar habilitados; el agent debe pertenecer a la `FULFILLMENT_BRANCH`; el `ND_ORDER_ITEM` se crea automáticamente.
- **Eliminación de order**: solo en estados reversibles (`OPEN`, `APPROVED`) y sin pago capturado ni facturación final; libera el inventory unit y produce `sale d` **más** `vehicle u` en la misma transacción.

## Estados y transiciones (sección 10)

| Proceso | NovaDrive |
|---|---|
| Customer activo | `A` |
| Customer suspendido | `S` |
| Customer cerrado | `X` |
| Inventory disponible | `AVL` |
| Inventory retenido | `HOLD` |
| Inventory vendido | `SOLD` |
| Order inicial | `OPEN` |
| Order aprobada | `APPROVED` |
| Order facturada | `INVOICED` |
| Order cancelada | `CANCELLED` |

`OPEN → APPROVED → INVOICED`; `OPEN` o `APPROVED` pueden pasar a `CANCELLED`. Los estados finales no retroceden. Una cancelación reversible libera el inventory unit (vuelve a `AVL`).

## Worker outbox (sección 13)

1. Buscar lote pequeño de `PENDING`/`RETRY` con `NEXT_RETRY_AT` vencido.
2. Reclamar filas con lock, pasarlas a `PROCESSING`.
3. Publicar `topic` + `kafka_key` + `payload`, esperar confirmación del broker.
4. Marcar `PUBLISHED` y completar `PUBLISHED_AT`.
5. Ante error: incrementar `ATTEMPTS`, guardar `LAST_ERROR`, aplicar backoff.
6. Tras el máximo de intentos: marcar `FAILED`, permitir reactivación manual.

**Una sola réplica**, compartida por los tres microservicios (ver `## Decisiones de diseño` — el worker no se divide por servicio, drena `ND_OUTBOX_EVENT` sin importar cuál microservicio escribió cada fila).

**VERIFICADO con evidencia real (9 sep 2026), no asumido** — lección heredada de Andes (en Oracle, `FOR UPDATE` + `LIMIT` revienta con `ORA-02014`, obligando a un reclamo en dos pasos). Contra Postgres 16 real (`novadrive-postgres-dev`), `EXPLAIN SELECT outbox_id FROM nd.outbox_event WHERE status='PENDING' AND next_retry_at <= now() ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 2;` planifica sin error (`Limit → LockRows → Sort → Index Scan`, usando `ix_outbox_status_retry_created`). Se probó además con **dos transacciones reales concurrentes** (`psql` sesión A y B, no solo el plan): sesión A reclama filas `{1,2}` y las retiene 4s con `pg_sleep`; sesión B arranca 1s después y reclama `{3,4}` — filas **distintas**, terminando en 0.36s, **sin esperar** a que A suelte el lock. Confirma que en Postgres **una sola consulta** (`FOR UPDATE SKIP LOCKED LIMIT n`) alcanza — el reclamo en dos pasos de Andes es complejidad heredada de Oracle que no aplica acá. `worker/outbox_worker.py` debe implementarse con esta consulta única, no portar el patrón de dos pasos.

## Estándar de interfaz web (acordado 9 sep 2026 — desde el diseño inicial, no como pulido final)

A diferencia de Andes (donde este estándar se pidió tarde y costó varias rondas), acá es un requisito desde la primera pantalla que se construya. No negociable:

1. **Identidad visual coherente**: paleta y tipografía consistentes en las tres pantallas, con espíritu profesional similar a Andes pero en inglés y con identidad propia de NovaDrive (paleta distinta a la de Andes, para que a simple vista se note que son dos sistemas distintos).
2. **Navegación entre las 3 entidades** (Customers / Inventory / Orders) desde una plantilla base compartida — el mismo `base.html`/CSS reutilizado en los tres microservicios (ver `## Decisiones de diseño` sobre cómo se comparte entre servicios separados).
3. **Dropdowns poblados con datos reales, nunca IDs a mano**: Inventory tiene dropdown de Branch; Order tiene dropdowns de Customer (solo `A` activo), Inventory (solo `AVL` disponible), Sales Agent (solo `ACTIVE_FLAG=1`, filtrado en el navegador por el Branch elegido) y Branch — todos poblados por consulta real al cargar la página, igual que Andes.
4. **Edición con prellenado, mismo criterio de diseño que Andes**: Customer e Inventory tienen edición completa con prellenado (reasignar cualquier campo). **Order es distinta a propósito**: su edición solo expone `ORDER_STATUS` (transición de estado) e `INVOICE_REFERENCE` — reasignar buyer/agent/inventory después de creada la orden rompería la disponibilidad ya comprometida del inventory unit y el invariante de "item creado automáticamente"; es la misma razón por la que Andes restringe la edición de venta a estado+factura, no una limitación nueva inventada para NovaDrive.
5. **Responsive real desde el inicio**, verificado en al menos 3 anchos de viewport (celular ~375px, laptop ~1366px, monitor grande ~1920px) antes de dar cualquier pantalla por cerrada — no ajustar a una sola resolución como pasó inicialmente en Andes.
6. **Manejo de errores de red visible**: timeout con `AbortController` (mismo patrón que `andesSubmit()` en Andes) y mensaje claro en banner ante error HTTP, red cortada o servidor colgado — nunca un botón que quede deshabilitado en silencio indefinidamente.
7. **Validación de negativos en campos monetarios/porcentuales desde el primer commit**, no como bug encontrado después: `GROSS_AMOUNT`, `TAX_AMOUNT`, `NET_AMOUNT`, `LIST_AMOUNT`, `UNIT_GROSS_AMOUNT`, `UNIT_NET_AMOUNT`, `CAPTURED_AMOUNT` ≥ 0; `DISCOUNT_PCT`, `LINE_DISCOUNT_PCT` entre 0 y 100 — validado con Pydantic en el schema de request, con manejo de `ValidationError` que devuelva 422 limpio (no 500 — Andes tuvo este bug exacto con `Decimal` + `jsonable_encoder`, ver lección en su propio CLAUDE.md; replicar el fix desde el día uno acá, no esperar a encontrarlo).

## Decisiones de diseño no especificadas por el documento (marcadas a propósito, no asumidas en silencio)

El PDF no dice cómo dividir "microservicios" en piezas concretas ni cómo generar códigos alfanuméricos — estas son decisiones tomadas para este proyecto, documentadas con su razón:

1. **Redpanda propio de NovaDrive**, no compartido con Andes — confirmado explícitamente el 9 sep 2026 por ser más simple de razonar/desplegar dado que cada proyecto vive en su propio VPS. Si aparece una razón técnica real para compartirlo, avisar antes de cambiarlo.
2. **Microservicios con base de datos compartida (no "database-per-service" estricto)**: tres servicios FastAPI independientes y desplegables por separado — `services/customers`, `services/inventory`, `services/orders` — pero los tres apuntan al **mismo** Postgres NovaDrive (un solo esquema `ND_*`). Razón: la sección 9, regla 6 exige "un solo commit; cualquier error revierte negocio y outbox" — esa atomicidad entre, por ejemplo, `ND_SALES_ORDER` + `ND_ORDER_ITEM` + `ND_PAYMENT_TXN` + el `UPDATE` de `ND_INVENTORY_UNIT.AVAILABILITY_CODE` + las filas de outbox, **no es alcanzable con bases de datos separadas por servicio** sin un patrón de saga/2PC — explícitamente fuera de alcance de este MVP. Por eso el servicio `orders` tiene permiso de escritura directa sobre `ND_INVENTORY_UNIT` (para el lock `FOR UPDATE` y el cambio de disponibilidad) y de lectura sobre `ND_CUSTOMER`/`ND_SALES_AGENT`/`ND_BRANCH` (para validar existencia/estado), dentro de su propia transacción — no llama a los otros servicios por HTTP para eso, porque eso rompería la atomicidad de un solo commit.
3. **Un solo worker de outbox, no uno por microservicio** — coherente con la sección 2 del PDF ("Publicación: Worker NovaDrive", singular) y con que las tres apps escriben a la misma tabla `ND_OUTBOX_EVENT`; dividir el worker por servicio no aportaría nada y complicaría el reclamo de lote.
4. **Navegación entre servicios sin gateway dedicado**: cada microservicio sirve su propia copia del `base.html`/CSS compartido (vive en `common/web/`, importado en build time por los tres) y los links de navegación usan URLs absolutas configurables por entorno (`CUSTOMERS_URL`, `INVENTORY_URL`, `ORDERS_URL` en `.env`) en vez de rutas relativas — así la experiencia visual es coherente sin necesitar un proxy inverso adicional que desplegar y mantener. Si en algún despliegue real conviene un gateway (Railway con un solo dominio público, por ejemplo), es un cambio de infraestructura, no de código de negocio.
5. **Generación de códigos de negocio** (el PDF no especifica el algoritmo): `CUSTOMER_CODE` autogenerado por la app (ej. prefijo `CUS-` + sufijo aleatorio/secuencial), `ORDER_NUMBER` autogenerado con patrón tipo `ORD-YYYYMMDD-HHMMSS-NN` (mismo espíritu que el número de factura autogenerado de Andes), `CHASSIS_NUMBER` es el VIN real que ingresa el usuario (no autogenerado), `BRANCH_CODE`/`AGENT_CODE` asignados por el script de siembra de datos de referencia. Sujeto a ajuste durante la implementación del checkpoint de modelos — se deja anotado acá para no reinventarlo a mitad de camino.
6. **`DISCOUNT_PCT`/`LINE_DISCOUNT_PCT` almacenados en puntos porcentuales** (ej. `10.5000` significa 10.5%, no `0.1050`) — igual convención que usaría cualquier lector humano del dato. Fórmula usada para `NET_AMOUNT`: `ROUND(GROSS_AMOUNT * (1 - DISCOUNT_PCT/100), 2) + TAX_AMOUNT`, análoga a nivel de línea para `UNIT_NET_AMOUNT`.
7. **Catálogo Databricks propio** `novadrive_catalog` con únicamente el schema `bronze` — ver razón en `## Alcance recortado`.
8. **Timestamps de negocio sin zona horaria (`TIMESTAMP(6)`), timestamps del outbox con zona horaria (`TIMESTAMPTZ(6)`)** — refleja literalmente la inconsistencia real del PDF entre las tablas `ND_*` (`LOCALTIMESTAMP`) y `ND_OUTBOX_EVENT` (`WITH TIME ZONE`); la app siempre calcula y escribe en UTC independientemente del tipo de columna.
9. **Identificadores físicos en Postgres en minúscula (`nd.customer`, `document_number`, ...), no `ND_CUSTOMER`/`DOCUMENT_NUMBER` literal.** Postgres pliega a minúscula cualquier identificador sin comillas — forzar mayúsculas literales obligaría a citar (`"ND_CUSTOMER"`) en cada query/ORM, lo cual es frágil y no idiomático en este motor (a diferencia de Oracle, donde mayúscula sin comillas es el comportamiento por defecto y por eso Andes sí usa `AM_CLIENTE` tal cual). Este CLAUDE.md sigue refiriéndose a las tablas por su nombre canónico `ND_*` del PDF (para trazabilidad con la sección 7), pero la tabla física real vive en el schema `nd` con nombre en minúscula: `nd.branch`, `nd.customer`, `nd.sales_agent`, `nd.inventory_unit`, `nd.sales_order`, `nd.order_item`, `nd.payment_txn`, `nd.outbox_event`. El campo `SOURCE_TABLE` del contrato de eventos sí guarda el nombre canónico en mayúscula (ej. `"ND_CUSTOMER"`) para mantener consistencia con el `AM_*` que emite Andes en el mismo campo.

## Estructura de carpetas propuesta

```
novadrive/
├── CLAUDE.md                          # este archivo
├── LECCIONES_TRANSVERSALES.md         # lecciones heredadas de Andes (referencia, no tareas pendientes)
├── .env.example                       # sin secretos reales
├── .gitignore                         # .env, .venv/, __pycache__/, .pytest_cache/
├── common/                            # paquete compartido por los 3 microservicios
│   ├── config.py                      # Pydantic Settings (Postgres, Kafka, Outbox, app)
│   ├── db.py                          # engine/session SQLAlchemy 2 + psycopg
│   ├── models/                        # 8 modelos ORM ND_* (uno por archivo)
│   ├── schemas/                       # Pydantic request/response + common.py (MutationResponse)
│   ├── events/                        # envelope, builder, topics, serialization (Decimal-como-string)
│   ├── outbox/                        # publisher.py — nunca hace commit
│   └── web/                           # base.html + CSS compartido, helpers Jinja
├── services/
│   ├── customers/                     # microservicio FastAPI — Customer CRUD
│   │   ├── main.py
│   │   ├── routes.py
│   │   └── templates/
│   ├── inventory/                     # microservicio FastAPI — Inventory CRUD
│   │   ├── main.py
│   │   ├── routes.py
│   │   └── templates/
│   └── orders/                        # microservicio FastAPI — Order (transacción cruzada, ver decisión #2)
│       ├── main.py
│       ├── routes.py
│       └── templates/
├── worker/
│   └── outbox_worker.py               # proceso único, compartido por los 3 servicios
├── bridge/
│   └── bridge_redpanda_to_databricks.py  # loop continuo → novadrive_catalog.bronze, append-only
├── db/
│   └── ddl_postgres.sql               # DDL de las 8 tablas ND_*
├── databricks/
│   └── sql/
│       └── ddl_bronze.sql             # solo bronze, sin silver/gold/quarantine
├── scripts/
│   └── seed_reference_data.py         # siembra ND_BRANCH / ND_SALES_AGENT
├── tests/                             # servicios, transacciones, eventos, rutas — sin dependencia de Postgres/Databricks vivos
├── docker-compose.local.yml           # Postgres + Redpanda (listener dual host/docker) para desarrollo local
├── Dockerfile                         # imagen común, entrypoint seleccionable (customers/inventory/orders/worker/bridge)
├── requirements.txt
└── pytest.ini
```

## Despliegue (sección 19, adaptado a microservicios)

| Servicio | Tipo | Notas |
|---|---|---|
| novadrive-customers | Web pública | Health check propio |
| novadrive-inventory | Web pública | Health check propio |
| novadrive-orders | Web pública | Health check propio, única con transacción cruzada (decisión #2) |
| novadrive-outbox-worker | Worker privado | Una réplica, compartida (decisión #3) |
| novadrive-bridge | Worker privado | Loop continuo hacia `novadrive_catalog.bronze` |

Una base Postgres (`ND_*`), un broker Redpanda, ambos propios de NovaDrive. Variables mínimas por grupo: igual esquema que Andes (Aplicación, Postgres en vez de Oracle, Redpanda, Outbox, Esquema) — ver sección 19 del PDF para la lista exacta, adaptando `ORACLE_*` a `POSTGRES_USER`/`POSTGRES_PASSWORD`/`POSTGRES_DSN` (sin wallet, Postgres no lo necesita).

## Regla de verificación de trabajo

**Ninguna afirmación de "esto funciona" se reporta sin evidencia real**: resultado real de una query, log real del worker/bridge (tópico/partición/offset), o respuesta HTTP real (status + body). Si algo no se pudo verificar, decirlo explícitamente — nunca asumir éxito por inferencia. Aplica a cada checkpoint.

## Hallazgos técnicos no obvios (NO redescubrir)

1. **Esta máquina de desarrollo tiene un Postgres nativo de Windows (`postgres.exe`) escuchando en el puerto 5432, además de Docker.** El primer intento de conectar (SQLAlchemy y también `psycopg.connect()` directo, con las credenciales correctas verificadas en `printenv` del contenedor) falló con `FATAL: la autentificación password falló para el usuario "novadrive_app"` — no porque la contraseña estuviera mal, sino porque `127.0.0.1:5432` resolvía contra ese Postgres nativo (PID distinto, proceso `postgres`, no `com.docker.backend`), que nunca oyó hablar de `novadrive_app`. Confirmado con `Get-NetTCPConnection -LocalPort 5432` + `Get-Process`: dos PIDs distintos escuchando el mismo puerto. **Solución aplicada**: el Postgres de NovaDrive en desarrollo local usa el puerto host **15432** (`docker-compose.local.yml` + `.env`/`.env.example`), no 5432. Si se despliega en un entorno sin ese Postgres nativo (VPS Linux limpio, por ejemplo), no hace falta este corrimiento de puerto — pero no asumirlo sin comprobar primero si el 5432 está libre ahí.
2. **`docker cp`/`docker exec` en Git Bash (MSYS) sobre Windows necesitan `MSYS_NO_PATHCONV` en momentos distintos, no ambos a la vez.** `docker cp <host-path> contenedor:/ruta` necesita la conversión de path normal de MSYS (sin `MSYS_NO_PATHCONV`) para resolver la ruta de Windows real; en cambio `docker exec ... psql -f /tmp/archivo.sql` necesita `MSYS_NO_PATHCONV=1` para que MSYS no reescriba `/tmp/archivo.sql` (ruta *dentro* del contenedor) como si fuera una ruta de Windows del host. Exportarlo para ambos a la vez rompe uno de los dos. Solución aplicada: `MSYS_NO_PATHCONV=1` solo como prefijo puntual en los comandos `docker exec`, nunca exportado de forma persistente en la sesión de shell.
3. **`SELECT ... FOR UPDATE SKIP LOCKED LIMIT n` SÍ funciona en una sola consulta en Postgres** — a diferencia de Oracle, donde `FOR UPDATE` + `LIMIT`/`FETCH FIRST` revienta con `ORA-02014` y obliga al reclamo en dos pasos que usa Andes. Verificado con dos transacciones Postgres reales concurrentes (no solo el plan de `EXPLAIN`): sesión A reclama y retiene filas 4s, sesión B arranca 1s después y reclama filas *distintas* sin esperar a que A suelte el lock (0.36s). `worker/outbox_worker.py::claim_batch()` usa esta única consulta — **no portar el patrón de dos pasos de Andes acá, no hace falta**.
4. **Validación de charset VIN (excluye I/O/Q) puesta en Pydantic desde el día uno**, no dejada para una capa posterior — deuda técnica real que Andes documentó y dejó abierta porque solo su capa Silver (inexistente en NovaDrive) la atrapaba. Como acá no hay Silver, dejar ese hueco habría significado que ningún VIN inválido se detectara nunca. Ver `services/inventory/schemas.py`.
5. **Bug de guard de PK por confundir el identificador de la URL con el del formulario**: si una ruta de edición (`POST /algo/{id}/edit`) arma el payload usando el `{id}` del *path* en vez de leer el campo homónimo del *body*, cualquier guard tipo "no se puede cambiar la PK" en la capa de servicio se vuelve código muerto — el valor del formulario nunca llega a compararse contra nada, así que el guard "pasa" aunque nunca se haya ejercitado de verdad. Encontrado en `services/inventory/routes.py` (edición de `chassis_number`): la primera prueba con curl "confirmó" el guard sin haberlo probado en serio. Antes de dar un guard de este tipo por verificado, confirmar explícitamente que el campo relevante se está leyendo del *body* del request, no del path.
6. **Filas del outbox pueden quedar atascadas en `PROCESSING` para siempre si el worker muere a mitad de ciclo** — `claim_batch()` solo reclama `PENDING`/`RETRY` por diseño, nunca `PROCESSING`, así que una fila que un worker marcó `PROCESSING` (con `commit()`) pero nunca llegó a resolver (crash, `docker rm -f`, kill) queda huérfana para siempre, sin que ningún worker futuro vuelva a intentarla. Reproducido con un caso real (un contenedor de prueba matado a propósito justo en ese punto), no solo razonado. **Fix de dos capas** en `worker/outbox_worker.py`: (1) *visibility timeout* — `claim_batch()` también reclama `PROCESSING` cuyo `claimed_at` supera `STALE_PROCESSING_SEC` (setting en `common/config.py`, default 120s); (2) `run_cycle()` envuelve `publish_one()` en `try/except` para que un fallo síncrono del cliente Kafka marque `RETRY` de inmediato en vez de depender solo del timeout.
7. ~~**Decisión de diseño del bridge: INSERT directo por lote a Bronze desde un proceso Python continuo, sin Volume/Auto Loader/Job de Databricks** — a diferencia de Andes. Esa infraestructura existe en Andes para las garantías de checkpointing/exactly-once que su Silver necesita; NovaDrive no tiene Silver, así que no hay nada que proteja.~~ **SUPERSEDIDA (9 sep 2026, Fase 14)** — el equipo de trabajo pidió explícitamente (requisito no negociable) que ambos sistemas hermanos suban los datos crudos de la misma manera; la razón técnica de arriba seguía siendo válida, pero se cambió igual por decisión de negocio, confirmada por CJ tras señalarle la contradicción. El bridge ahora sube archivos `.jsonl` a un Volume, igual que Andes — ver Fase 14 y hallazgo técnico #17. Consecuencia aceptada que **sigue vigente** con el nuevo esquema también: puede haber duplicados en los archivos ante reintento, cubierto por la misma regla del contrato — no requiere deduplicación de este lado tampoco.
8. **`rpk cluster health --brokers ...` no existe como comando** — ese subcomando de `rpk` no acepta `--brokers`; usa el perfil local de rpk dentro del propio contenedor. Un healthcheck de Redpanda que lo use falla con `unknown flag` y bloquea (vía `depends_on: condition: service_healthy`) el arranque de todo lo que dependa de él. Usar `rpk cluster health` a secas.
9. **Un solo listener de Redpanda no es alcanzable desde otro contenedor** (mismo problema de fondo que Andes documentó: el protocolo Kafka redirige a la dirección *anunciada*, y una dirección pensada para el host —`127.0.0.1:<puerto>`— no existe dentro de otro contenedor). Confirmado acá con un error real (`Connection refused`) en un `docker run` suelto de un solo listener, antes de escribir el `docker-compose.yml` definitivo — por eso el compose final ya nace con **dos listeners** (`INTERNAL://redpanda:9092` para contenedores, `EXTERNAL://127.0.0.1:<puerto>` para el host), aplicado directamente desde el diseño en vez de tener que redescubrir el mismo error dentro del compose.
10. **Un script que parte un archivo `.sql` por `;` puede descartar una sentencia completa en silencio si su primera línea es un comentario `--`** — el error típico es comprobar `statement.strip().startswith('--')` sobre el bloque entero (que incluye la sentencia real después del comentario) en vez de filtrar comentario por línea antes de unir cada sentencia. Se manifiesta como un error de "no existe tal objeto" en la sentencia *siguiente*, no en la que realmente se perdió — si eso pasa, revisar primero si el script de aplicación de DDL está descartando algo, no asumir que el DDL en sí está mal escrito.
11. **`enable.auto.commit=True` en un consumer que bufferea en memoria antes de persistir puede perder mensajes, no solo duplicarlos** — hallazgo real en `bridge/bridge_redpanda_to_databricks.py` (9 sep 2026, a raíz de arreglar el hallazgo #12): con auto-commit, Kafka avanza el offset commiteado por el simple hecho de que `poll()` devolvió el mensaje, sin que le importe si ese mensaje ya llegó a Bronze o todavía está solo en el `buffer` en memoria. Si el proceso terminaba entre el `poll()` y un `upload_batch()` exitoso (antes: una excepción no atrapada; ahora: el propio `raise` a propósito tras `MAX_CONSECUTIVE_FAILURES`, ver #12), ese mensaje se perdía para siempre — nunca se releía tras el reinicio porque Kafka ya lo daba por consumido. Contradice el contrato *at-least-once* de la sección 11 del PDF. **Fix**: `enable.auto.commit=False` + `consumer.commit()` manual, solo inmediatamente después de un `upload_batch()` exitoso — en el peor caso un reinicio reprocesa el mismo lote (duplicados en Bronze, ya esperados y aceptados por el contrato), nunca lo pierde. **Verificado con un fallo real forzado** (`DATABRICKS_WAREHOUSE_ID` apuntado a un warehouse inexistente a propósito, 3 ciclos de crash-loop reales): el mismo evento se mantuvo en "1 buffered event" sin crecer ni perderse en los 3 reinicios, y al restaurar la configuración correcta se confirmó con una query real a `novadrive_catalog.bronze.events` que llegó **exactamente una vez** (`COUNT(*) = 1`), no cero ni más de una.
12. **Un `except Exception` sin límite alrededor de un flush que puede fallar para siempre es un loop de reintentos silencioso, no una recuperación** — `bridge/bridge_redpanda_to_databricks.py::loop_continuous()` atrapaba cualquier fallo de `upload_batch()` indefinidamente (`logger.exception` + `sleep(2)` + `continue`), sin caerse nunca — visto como algo "seguro" en el momento, pero en la práctica significa que un problema real (tabla/catálogo inexistente, credencial vencida) queda invisible salvo que alguien revise `docker logs` a propósito, y el `buffer` en memoria crece sin límite mientras tanto. **Fix**: contador de fallos consecutivos (`MAX_CONSECUTIVE_FAILURES`, default 8, env `BRIDGE_MAX_CONSECUTIVE_FAILURES`) que, al superarse, loguea `CRITICAL` y deja que la excepción se propague y el proceso termine — `docker-compose.yml` ahora tiene `restart: unless-stopped` en `bridge` (no lo tenía, a diferencia de Andes) para que Docker lo reinicie solo, visible en `docker ps`/`docker logs`, igual que el bridge de Andes. Tope adicional de `buffer` en memoria (`MAX_BUFFER_SIZE`, default 2000, env `BRIDGE_MAX_BUFFER_SIZE`): al llegar al tope, el bridge deja de hacer `poll()` (backpressure) en vez de seguir acumulando — los mensajes no leídos quedan intactos en Redpanda. **Verificado con evidencia real**: fallo forzado con `MAX_CONSECUTIVE_FAILURES=2` real → log exacto `1/2` → `2/2` → `CRITICAL` → el contenedor termina y Docker lo reinicia solo (`docker inspect --format '{{.RestartCount}}'` subió a 2 tras dos ciclos completos, `docker ps` mostró el contenedor con uptime reiniciado) — reproducido 3 veces seguidas mientras la configuración siguió rota, confirmando un crash-loop real y visible, no hipotético.
13. **`DATABRICKS_WAREHOUSE_ID` tenía un default hardcodeado al warehouse de Andes (`548c86efc1164b8d`) en `bridge/databricks_sql.py` y en `docker-compose.yml`** — funcionaba "por casualidad" porque hoy ambos proyectos comparten workspace (ver decisión #7), pero apuntar `DATABRICKS_HOST`/`DATABRICKS_TOKEN` a un workspace nuevo sin también cambiar esta variable habría consultado en silencio el warehouse equivocado (o uno inexistente ahí). **Fix**: sin default en el código (`os.environ["DATABRICKS_WAREHOUSE_ID"]`, falla rápido con `KeyError` si falta) ni en el compose (`${DATABRICKS_WAREHOUSE_ID}` a secas); el valor vive ahora explícito en `.env`/`.env.example`, documentado como "el único lugar que hay que tocar si cambia el workspace".
14. **El `mem_limit` de un contenedor debe quedar POR ENCIMA del `--memory` interno que se le pasa a Redpanda, nunca igual** — encontrado real en el primer intento de despliegue al VPS (9 sep 2026): con `mem_limit: 512m` igual al `--memory 512M` de la línea de comando de Redpanda, el proceso fallaba al arrancar (`docker ps` en crash-loop real) con `Could not initialize seastar: std::runtime_error (insufficient physical memory: needed 536870912 available 500000000)` — el resto del contenedor (el propio `rpk`, el healthcheck) ya consume parte de ese cgroup antes de que Redpanda intente reservar sus 512MB completos, así que nunca llegaban a estar disponibles los 512MB exactos que pedía. **Fix**: `mem_limit` subido a `640m` en `docker-compose.prod.yml` (128MB de margen), misma proporción que ya usaba `docker-compose.yml` de desarrollo (`mem_limit: 1200m` para `--memory 1G`) — ese archivo nunca tuvo el bug porque ya dejaba margen, no fue diseño consciente hasta que este segundo caso lo hizo explícito.
15. **Una cuenta de Databricks trial puede agotar su crédito ($ fijo, no rotativo) a mitad de sesión, y el bloqueo es a nivel de workspace, no solo del SQL Warehouse que se esté usando** — pasó real (9 sep 2026) con la cuenta personal usada hasta ese momento: el warehouse pasó a `STOPPED` y cualquier intento de arrancarlo (`POST /api/2.0/sql/warehouses/{id}/start`) devolvía `400 BAD_REQUEST` con `reason: CREDIT_EXHAUSTED, domain: resource-gatekeeper, DENY_NEW_AND_EXISTING_RESOURCES` — un bloqueo de la plataforma, no un límite de cuota que se pueda subir con una config. El panel de "manage trial" del workspace (ej. "$3 de $40 restantes, actualizado hace 7 horas") **no es un contador que se reinicie** — ese "hace 7 horas" es solo la frescura de los datos de uso mostrados (con retraso típico de varias horas), no una cuenta regresiva; el crédito es un paquete único que no se recarga solo. Prevención futura: si se vuelve a depender de una cuenta trial personal para una demo, chequear el panel de crédito con margen de tiempo, no asumir que "algo de crédito restante" alcanza para terminar la sesión.
16. **Un ID de Databricks (Workspace ID vs. Warehouse ID vs. warehouse-de-otro-workspace) nunca se debe asumir válido solo porque "tiene forma de ID" — hay que verificarlo con una llamada real a la API antes de cargarlo en ningún `.env`.** Pasó real durante la migración de cuenta de Databricks (9 sep 2026), dos veces seguidas antes del valor correcto: (1) un ID de 16 dígitos numéricos que resultó ser el Workspace ID, no un Warehouse ID (`GET /api/2.0/sql/warehouses/{id}` → `404`); (2) un ID con formato correcto (hexadecimal de 16 caracteres) pero que pertenecía a un workspace distinto al `DATABRICKS_HOST` que se tenía cargado en ese momento (`404` otra vez, aunque el host/token sí eran válidos y permitían listar warehouses — solo que ese ID en particular no estaba en la lista). **Los IDs de warehouse solo son únicos dentro de su propio workspace** — nunca alcanza con el ID solo, hace falta el host correcto también, y ambos se verifican juntos con `GET /api/2.0/sql/warehouses/{id}` contra ese host antes de asumir que un despliegue va a funcionar.
17. **La Unity Catalog REST API (`/api/2.0/unity-catalog/{catalogs,schemas,volumes}`) sí permite crear catálogo/schema/Volume sin arrancar ningún SQL Warehouse** — confirmado real (9 sep 2026, migración a Volume+`.jsonl`, Fase 14): `POST /api/2.0/unity-catalog/schemas` y `POST /api/2.0/unity-catalog/volumes` funcionaron con el warehouse en `STOPPED` todo el tiempo, sin ningún intento de arranque — a diferencia de la Statement Execution API (`CREATE CATALOG/SCHEMA/VOLUME` por SQL), que sí necesita un warehouse corriendo. Esto no estaba verificado en ningún proyecto anterior (ni siquiera Andes, que usó la vía SQL) — se confirmó acá con `GET` real de cada objeto después de crearlo, no asumido de la documentación. Útil para cualquier operación de solo-metadata de Unity Catalog futura: preferir esta API sobre la de SQL cuando no haga falta ejecutar una consulta real, cero costo de cómputo.

## Estado exacto de avance

- **Fase 0 — Alcance y documento normativo**: CLAUDE.md creado (9 sep 2026), alcance recortado confirmado por escrito con quien encargó el proyecto y el líder de equipo, decisiones de diseño no especificadas por el PDF marcadas explícitamente. `_legacy-referencia-novadrive/` (intento anterior con Silver/Gold) identificado y confirmado como material a NO tocar ni usar como base.
- **Fase 1 — Modelo relacional Postgres, completa y verificada con evidencia real (9 sep 2026)**:
  - `db/ddl_postgres.sql` con las 8 tablas `nd.*` (nombres físicos en minúscula, ver decisión #9), aplicado contra **Postgres 16 real** (`postgres:16-alpine`, contenedor `novadrive-postgres-dev`, healthcheck en 5s).
  - **Evidencia real, no "debería funcionar"**: `information_schema.tables` confirma las 8 tablas; conteo de constraints por tabla confirma PK en las 8, FK en `sales_agent`/`inventory_unit`/`sales_order`(×3)/`order_item`(×2)/`payment_txn`, UNIQUE en `customer.document_number` y `order_item.chassis_number`, CHECK de negativos/rangos en todos los campos monetarios y de porcentaje.
  - **Tipos verificados por query real**: columnas de negocio (`sales_order.ordered_at`, `.created_on`) → `timestamp without time zone`; columnas del outbox (`outbox_event.created_at/occurred_at/next_retry_at`) → `timestamp with time zone` — confirma la decisión #8 tal cual quedó documentada, no al revés.
  - **Constraints probadas de verdad, no solo declaradas**: `INSERT` con `gross_amount=-100` → `ERROR: ... violates check constraint "ck_sales_order_gross_amount"`; `INSERT` con `document_number` duplicado → `ERROR: ... violates unique constraint "uq_customer_document"`; `INSERT` en `inventory_unit` con `branch_code` inexistente → `ERROR: ... violates foreign key constraint "fk_inventory_unit_branch"`.
  - **Reclamo del worker outbox verificado con dos transacciones concurrentes reales** — ver detalle y timestamps en `## Worker outbox`: una sola consulta `FOR UPDATE SKIP LOCKED LIMIT n` alcanza en Postgres, no hace falta el patrón de dos pasos de Oracle.
  - `docker-compose.local.yml` creado y probado desde cero: contenedor recreado vía `docker compose up -d`, el DDL se auto-aplica por `docker-entrypoint-initdb.d` (confirmado en el log del contenedor: `running /docker-entrypoint-initdb.d/01_ddl_postgres.sql`), las 8 tablas aparecen limpias tras el arranque.
  - Datos de prueba de esta verificación ya truncados (`TRUNCATE ... RESTART IDENTITY CASCADE`) — el esquema queda limpio para la siembra real de datos de referencia.
  - **Pendiente explícito para el siguiente paso**: modelos ORM SQLAlchemy 2 en `common/models/` (uno por tabla, mapeando a `nd.*`), y el paquete `common/config.py`/`common/db.py` (engine/session Postgres) — el DDL ya existe y está verificado, falta la capa ORM que lo consume.
- **Fase 2 — Modelos ORM SQLAlchemy 2, completa y verificada con evidencia real (9 sep 2026)**:
  - `common/config.py` (Pydantic Settings, `POSTGRES_*`/`KAFKA_*`/`OUTBOX_*`), `common/db.py` (engine `postgresql+psycopg`, `search_path=nd`, `SessionLocal`, `get_session()`), y los 8 modelos en `common/models/` (uno por tabla, `Base.metadata = MetaData(schema="nd")` — mapean el esquema ya aplicado en Fase 1, no lo generan, mismo principio que Andes).
  - Entorno real: venv Python 3.12.5, `sqlalchemy==2.0.35`, `psycopg[binary]==3.2.3`, `pydantic-settings==2.5.2`.
  - **Evidencia real vía `scripts/checkpoint_orm_postgres.py`** (script desechable, no runtime) corrido contra el Postgres real de `docker-compose.local.yml`, todo con salida real capturada, no supuesta:
    - INSERT de las 8 tablas en orden de dependencia, con `flush()` intermedio y `commit()` real.
    - SELECT con navegación por `relationship()`, incluida una relación cruzada de dos saltos (`sales_order.items[0].inventory_unit.brand_name`).
    - UPDATE real de transición de estado (`OPEN → APPROVED`), releído tras `expire_all()`.
    - **CHECK constraint violada a propósito vía ORM** (`gross_amount=-100`) → `IntegrityError` real con el nombre exacto de la constraint (`ck_sales_order_gross_amount`) en el mensaje, `rollback()`, y confirmación de que la sesión sigue utilizable después (el dato bueno previo seguía intacto) — no solo "se esperaba un error", sino el mensaje real capturado y la sesión recuperada.
    - DELETE en orden inverso de dependencias, esquema `nd` verificado limpio al final (`count() == 0`).
  - **Hallazgo real de entorno encontrado y resuelto en el camino** (no es un problema del código ORM): esta máquina tiene un Postgres nativo de Windows compitiendo por el puerto 5432 — ver `## Hallazgos técnicos no obvios` #1. Sin ese diagnóstico, el error de autenticación se habría atribuido erróneamente a una contraseña mal generada o mal leída por Pydantic Settings (ambas se verificaron explícitamente como correctas antes de sospechar del puerto).
- **Fase 3 — Contrato de eventos (`common/events/`) y schemas compartidos (`common/schemas/`), completa y verificada con 32 tests reales (9 sep 2026)**:
  - `common/events/topics.py` (mapa entidad→`ND_*`/tópico/clave, `SOURCE_SYSTEM="NOVADRIVE"`), `envelope.py` (`EventEnvelope` Pydantic, `source_system: Literal["NOVADRIVE"]`), `builder.py` (`build_event()` con la validación `c`/`u`/`d` de la sección 11), `serialization.py` (`dumps_decimal_safe` — Decimal como STRING JSON, igual decisión que Andes, mismo contrato compartido).
  - `common/schemas/common.py` (`EventRef`/`MutationResponse`, forma de respuesta de la sección 8) y `common/schemas/validators.py` (`NonNegativeAmount`, `DiscountPercentage` — tipos Pydantic reutilizables para no repetir `Field(ge=0)`/`Field(ge=0, le=100)` en cada servicio; nacen directo del estándar de interfaz del 9 sep 2026, antes de que exista ninguna ruta que los use).
  - **pytest real, 32/32 passed, output completo capturado** (`tests/test_events_builder.py`, `test_events_serialization.py`, `test_events_topics.py`, `test_schemas_validators.py`):
    ```
    tests/test_events_builder.py::test_creacion_c_exige_before_none_y_after_presente PASSED [  3%]
    tests/test_events_builder.py::test_creacion_c_con_before_no_none_falla PASSED [  6%]
    tests/test_events_builder.py::test_actualizacion_u_exige_before_y_after PASSED [  9%]
    tests/test_events_builder.py::test_actualizacion_u_sin_before_o_sin_after_falla[None-after0] PASSED [ 12%]
    tests/test_events_builder.py::test_actualizacion_u_sin_before_o_sin_after_falla[before1-None] PASSED [ 15%]
    tests/test_events_builder.py::test_actualizacion_u_sin_before_o_sin_after_falla[None-None] PASSED [ 18%]
    tests/test_events_builder.py::test_eliminacion_d_exige_after_none_y_before_presente PASSED [ 21%]
    tests/test_events_builder.py::test_eliminacion_d_con_after_no_none_falla PASSED [ 25%]
    tests/test_events_builder.py::test_entidad_desconocida_falla PASSED      [ 28%]
    tests/test_events_builder.py::test_occurred_at_por_defecto_es_utc_ahora PASSED [ 31%]
    tests/test_events_builder.py::test_dos_eventos_del_mismo_correlation_id_tienen_event_id_distintos PASSED [ 34%]
    tests/test_events_serialization.py::test_decimal_se_serializa_como_string_no_como_numero_json PASSED [ 37%]
    tests/test_events_serialization.py::test_decimal_clasico_trampa_de_float_no_pierde_precision PASSED [ 40%]
    tests/test_events_serialization.py::test_decimal_de_16_2_precision_exacta_no_se_trunca PASSED [ 43%]
    tests/test_events_serialization.py::test_discount_pct_numeric_7_4_conserva_los_4_decimales PASSED [ 46%]
    tests/test_events_serialization.py::test_datetime_se_serializa_iso8601_utc PASSED [ 50%]
    tests/test_events_serialization.py::test_datetime_naive_se_asume_utc PASSED [ 53%]
    tests/test_events_serialization.py::test_tipo_no_soportado_lanza_typeerror PASSED [ 56%]
    tests/test_events_topics.py::test_topicos_exactos_seccion_12 PASSED      [ 59%]
    tests/test_events_topics.py::test_source_table_por_entidad PASSED        [ 62%]
    tests/test_events_topics.py::test_kafka_key_formato_exacto PASSED        [ 65%]
    tests/test_events_topics.py::test_kafka_key_misma_clave_para_mismo_registro PASSED [ 68%]
    tests/test_events_topics.py::test_no_hay_topicos_distintos_para_c_u_d PASSED [ 71%]
    tests/test_schemas_validators.py::test_monto_negativo_rechazado PASSED   [ 75%]
    tests/test_schemas_validators.py::test_monto_cero_permitido PASSED       [ 78%]
    tests/test_schemas_validators.py::test_monto_positivo_permitido PASSED   [ 81%]
    tests/test_schemas_validators.py::test_descuento_negativo_rechazado PASSED [ 84%]
    tests/test_schemas_validators.py::test_descuento_mayor_a_100_rechazado PASSED [ 87%]
    tests/test_schemas_validators.py::test_descuento_en_rango_valido_permitido[valor0] PASSED [ 90%]
    tests/test_schemas_validators.py::test_descuento_en_rango_valido_permitido[valor1] PASSED [ 93%]
    tests/test_schemas_validators.py::test_descuento_en_rango_valido_permitido[valor2] PASSED [ 96%]
    tests/test_schemas_validators.py::test_error_de_validacion_es_serializable_a_json_via_jsonable_encoder PASSED [100%]

    ============================= 32 passed in 0.47s ==============================
    ```
  - El último test (`test_error_de_validacion_es_serializable_a_json_via_jsonable_encoder`) reproduce **a propósito, antes de que ocurra** el hallazgo #13 de Andes (un `ValidationError` con un `Decimal` inválido revienta en 500 si se serializa `exc.errors()` crudo) — verificado aislado con `jsonable_encoder`, sin FastAPI de por medio todavía. El manejo real de excepciones en las rutas (Fase 4) debe usar este mismo patrón, no `exc.errors()` a secas.
  - `fastapi==0.115.0` instalado en el venv (necesario para `jsonable_encoder` en el test anterior, no todavía usado en runtime — eso es Fase 4).
- **Fase 4a — Microservicio Customer completo, primero de los tres, verificado end-to-end contra Postgres real (9 sep 2026)**:
  - **Piezas compartidas nuevas** (usadas también por Inventory/Order más adelante): `common/security.py` (HTTP Basic, `secrets.compare_digest`), `common/errors.py` (`NotFoundError`/`ConflictError`/`ValidationDomainError`), `common/outbox/publisher.py` (`enqueue_outbox_event`, nunca hace commit), `common/web/templates.py` (Jinja2Templates con **dos** carpetas de búsqueda — `common/web/` + la del servicio — así `base.html` vive en un solo lugar sin duplicarse por servicio, ver decisión #4), `common/web/base.html` (identidad visual propia de NovaDrive: navy `#0b1424` + acento teal `#14b8a6`, deliberadamente distinta a la de Andes; nav con URLs absolutas `nav.customers_url`/etc. inyectadas como globals de Jinja desde `Settings`), `common/web/routes_events.py` (`GET /events/{event_id}/status`, compartido por los tres — todos leen la misma `nd.outbox_event`).
  - **`services/customers/`**: `schemas.py` (`CustomerEntrada`/`CustomerOut`), `repository.py` (incluye `has_orders()` — lee `nd.sales_order` directo, sin llamada HTTP a Orders, ver decisión #2), `service.py` (`create_customer`/`edit_customer`/`delete_customer`, un solo commit negocio+outbox, código de negocio `CUS-` + `secrets.token_hex` con reintento anti-colisión), `routes.py` (los 3 POST de la sección 8 + manejo 422 con `jsonable_encoder`), `main.py` (`/health` sin auth), `templates/customers.html` (dropdown de estado A/S/X — Customer no referencia otras entidades, así que no necesita dropdown de datos externos; esa necesidad llega con Inventory/Order).
  - **Verificado con HTTP real contra el servicio real (`uvicorn` en `127.0.0.1:8001`) + queries reales a Postgres**, no simulado:
    - `POST /customers` real → `201`-equivalente con `correlation_id`+`event_id`; fila real confirmada en `nd.customer` y evento real en `nd.outbox_event` (`SOURCE_TABLE=ND_CUSTOMER`, `KAFKA_TOPIC=cdc.novadrive.customer`, `KAFKA_KEY=NOVADRIVE|customer|CUS-...`, `PAYLOAD_JSON` con `before=null`/`after` completo).
    - `POST /customers/{code}/edit` real → evento `u` con `before`/`after` **distintos** confirmados por query (`before_name="Jamie Chen"`, `after_name="Jamie R. Chen"`).
    - Documento duplicado → **409 real** (`"A customer with document ... already exists"`).
    - Email inválido → **422 real, limpio** (no 500 — el `jsonable_encoder` funcionó tal como se probó aislado en Fase 3).
    - Sin credenciales → **401 real**.
    - `POST /customers/{code}/delete` real → fila desaparece de `nd.customer` (`count()==0`), pero **los 3 eventos `c`/`u`/`d` siguen íntegros en `nd.outbox_event`** (confirma que el outbox sobrevive al borrado del origen, tal como exige el diseño sin FK).
    - `GET /events/{event_id}/status` real → estado `PENDING` correcto (el worker todavía no existe, es lo esperado); `event_id` inexistente → `404` real.
    - **Regla "no eliminar customer con órdenes" verificada con evidencia real** (`scripts/checkpoint_customer_delete_conflict.py`, fixture de orden vía ORM directo porque Orders todavía no tiene microservicio — única excepción deliberada a "no DML manual", documentada en el propio script): `DELETE` con orden asociada → **409 real**; tras liberar la orden, el mismo `DELETE` → **200 real**.
  - **Responsive verificado con Playwright real, no supuesto** (`scripts/checkpoint_customers_responsive.py`): 375×812 (phone), 1366×768 (laptop), 1920×1080 (wide monitor) — `document.documentElement.scrollWidth == clientWidth` en los 3 (sin scroll horizontal de página). Verificación adicional en phone: la tabla sí necesita scroll horizontal *interno* (`.table-scroll`, `scrollWidth=413` vs `clientWidth=322`) para llegar a la columna Actions — confirmado con Playwright que haciendo `scrollLeft` dentro de ese contenedor la columna Edit/Delete aparece completa; es el comportamiento esperado del patrón (contenido ancho scrollea en su propio contenedor, nunca la página), no una columna perdida.
  - Manejo de fallos de red (`novadriveSubmit`, timeout 12s vía `AbortController`) y dropdowns-no-IDs-a-mano quedan resueltos por diseño en este primer servicio (Customer no tiene FKs hacia otra entidad); ambos deben reverificarse con datos reales de Branch/Agent cuando se construya Inventory/Order.
  - `playwright==1.47.0` + Chromium instalados en el venv (dev-only, no es parte del stack de runtime de la app).
- **Fase 4b — Microservicio Inventory completo, verificado end-to-end contra Postgres real (9 sep 2026)**:
  - `services/inventory/`: `schemas.py` (`InventoryEntrada`/`InventoryOut`/`BranchOption`), `repository.py` (`list_enabled_branches` para el dropdown, `is_in_an_order` lee `nd.order_item` directo), `service.py` (chasis = PK, sin generación de código — a diferencia de Customer, lo escribe el usuario), `routes.py`, `main.py`, `templates/inventory.html` (dropdown de Branch poblado por consulta real, nunca un campo de texto libre para `branch_code`).
  - **Cierre proactivo de una deuda técnica real de Andes**: `chassis_number` valida el charset VIN completo (excluye I/O/Q) desde Pydantic — Andes dejó ese hueco abierto porque solo Silver lo validaba (documentado en su CLAUDE.md como deuda técnica); como NovaDrive no tiene Silver, ese hueco habría dejado pasar VIN inválidos sin detectarlos nunca. Verificado con HTTP real: `1HGCM82633A0O4352` (una `O` en vez de `0`) → **422 real** con el mensaje exacto de qué caracter es inválido.
  - `scripts/seed_reference_data.py` — siembra `nd.branch`/`nd.sales_agent` (3 sedes, 4 agentes), idempotente, sin outbox (dato de referencia, igual criterio que Andes con `AM_SUCURSAL`/`AM_VENDEDOR`).
  - **Bug real encontrado y corregido en este mismo checkpoint, no en una ronda posterior**: la ruta de edición pasaba el `chassis_number` de la **URL** como si fuera el valor enviado en el formulario, así que el guard `service.edit_inventory_unit` ("no se puede cambiar la PK") era código muerto — mi primera prueba con curl "confirmó" el guard sin haberlo ejercitado de verdad (el campo del formulario se ignoraba en silencio). Encontrado al revisar la propia evidencia antes de darla por buena, no asumido. **Fix**: la ruta ahora lee `chassis_number` del cuerpo del formulario por separado (`chassis_number_body: str = Form(..., alias="chassis_number")`) y se lo pasa al servicio junto con el de la URL, para que la comparación sea entre dos valores realmente independientes. Reverificado: edición legítima → `200`; intento real de cambiar la PK en el body → **422 real** (`"chassis_number cannot be changed on edit; it is the primary key"`).
  - **Verificado con HTTP real + queries reales a Postgres** (servicio en `127.0.0.1:8002`): dropdown de Branch poblado por consulta real (`GET /inventory` devuelve las 3 sedes sembradas); creación con fila+evento reales (`SOURCE_TABLE=ND_INVENTORY_UNIT`, tópico `cdc.novadrive.vehicle`); monto negativo → 422 real; branch inexistente/deshabilitado → **422 real de regla de negocio** (`ValidationDomainError`, no solo de forma); chasis duplicado → 409 real; edición real con `before`/`after` distintos.
  - **Regla "no eliminar si ya pertenece a una orden" verificada con fixture real** (`scripts/checkpoint_inventory_delete_conflict.py`, misma excepción documentada a "no DML manual" que en Customer — `ND_ORDER_ITEM` creado por ORM porque Orders no existe todavía): 409 con item asociado, 200 tras liberarlo.
  - **Responsive verificado con Playwright real**: 375×812/1366×768/1920×1080, sin scroll horizontal de página en ninguno (incluye el dropdown de Branch y el formulario con 8 campos).
  - Manejo de fallos de red: `inventory.html` reutiliza `novadriveSubmit()` de `common/web/base.html` sin modificarlo — mismo timeout/banner ya verificado en Fase 4a, no se re-testeó el caso de red cortada aquí porque el código es literalmente el mismo, no una reimplementación.
- **Fase 4c — Microservicio Orders completo, el más complejo de los tres, verificado end-to-end contra Postgres real (9 sep 2026)**:
  - `services/orders/`: `schemas.py` (`OrderEntrada`/`OrderEdicion`/`OrderOut` + 4 schemas de opciones de dropdown), `repository.py` (única excepción arquitectónica documentada: lee y bloquea `InventoryUnit` con `FOR UPDATE` directo, fuera de sus "propias" tablas — ver decisión #2), `service.py` (`create_order`/`edit_order`/`delete_order`, número de orden autogenerado `ORD-YYYYMMDD-HHMMSS-XXXX`, fórmula de descuento porcentual `net_amount = round(gross*(1-pct/100),2) + tax` tal como quedó documentada en la decisión #6), `routes.py`, `main.py`, `templates/orders.html` (4 dropdowns: Branch, Customer activo, Sales Agent activo filtrado por sede, Inventory disponible filtrado por sede — filtrado 100% client-side sin round-trip extra, mismo patrón que Andes con Vendedor).
  - **Transacción cruzada completa verificada con evidencia real** (servicio en `127.0.0.1:8003`, contra customer e inventory unit creados previamente vía HTTP real en los otros dos servicios): un solo `POST /orders` produjo, en un solo commit, fila real en `nd.sales_order` (`net_amount=30500.00` = `32000×(1−0.05)+100`, verificado con la fórmula exacta), fila real en `nd.order_item`, fila real en `nd.payment_txn`, y el `UPDATE` de `nd.inventory_unit.availability_code` a `SOLD` — los 5 cambios confirmados con 5 queries reales independientes, no solo el body de la respuesta HTTP. Los dos eventos (`sale c` + `vehicle u`) comparten el mismo `correlation_id`, confirmado por query.
  - **Doble venta verificada con concurrencia real, no secuencial** (`scripts/checkpoint_double_sale.py`): dos threads sincronizados con `threading.Barrier` (para que ambos `POST /orders` salgan lo más simultáneo posible, no uno tras otro) contra el mismo `chassis_number`. Resultado real: exactamente un `200` y un `409` (nunca dos `200` ni dos `409`). **La evidencia que de verdad importa, tal como pediste, no es el código HTTP sino el conteo antes/después**: `sales_order` pasó de 1 a 2 (exactamente +1, no +2), `order_item` para ese chasis de 0 a 1 — confirmado con `COUNT()` real contra Postgres antes y después del disparo concurrente, no inferido del status code. La unidad terminó `SOLD` una sola vez, sin estado intermedio inconsistente. El `FOR UPDATE` de `get_inventory_unit_for_update` es lo que serializa la segunda transacción hasta que la primera libera el lock — el segundo thread ve la unidad ya `SOLD` y falla limpio.
  - **Máquina de estados verificada con HTTP real, incluidas las transiciones inválidas**: `OPEN→APPROVED` (200) → `APPROVED→INVOICED` (200, con `invoice_reference`) → editar una orden ya `INVOICED` (409, estado final) → intentar `APPROVED→OPEN` (409, no hay retroceso) → eliminar una orden `INVOICED` (409, no es estado reversible).
  - **Eliminación reversible verificada de punta a punta**: orden `OPEN` sin pago capturado → `DELETE` real → confirmado por query que la orden y su item desaparecieron, y que `nd.inventory_unit.availability_code` volvió a `AVL` (liberación real, no solo la respuesta HTTP) — con los eventos `sale d` + `vehicle u` reales bajo el mismo `correlation_id`.
  - **Regla "no eliminar con pago capturado" verificada con fixture real** (`scripts/checkpoint_order_captured_payment_conflict.py` — única excepción deliberada a "no DML manual", documentada: solo `captured_at` se setea por ORM porque este MVP no tiene integración real de pasarela de pago que lo haga solo): orden con pago recién creado (sin capturar) es eliminable; tras marcar `captured_at`, el mismo `DELETE` → **409 real**.
  - **Filtrado de dropdowns por sede verificado con Playwright de verdad, en ambos sentidos** (no solo "no truena"): al elegir `BR01`, el dropdown de Agent mostró `AG01`/`AG02` y ocultó `AG03` (de `BR02`); al alternar entre `BR01`/`BR02` con dos unidades `AVL` sembradas una en cada sede, el dropdown de Inventory mostró la unidad correcta y ocultó la otra en cada caso — confirmado leyendo el DOM real (`option:not([hidden])`), no asumido por el código JS.
  - **Responsive verificado con Playwright real**: 375×812/1366×768/1920×1080, sin scroll horizontal de página en ninguno, con el formulario de 10 campos + 4 dropdowns.
  - `pytest tests/` sigue en 32/32 tras las tres fases de microservicios (los checkpoints de servicios son scripts de integración contra Postgres/HTTP reales, no pytest — mismo criterio que Andes: pytest cubre lo que no depende de infraestructura viva).
- **Los tres microservicios de NovaDrive (Customer, Inventory, Order) están completos y verificados end-to-end.**
- **Fase 5 — Redpanda propio de NovaDrive levantado y verificado (9 sep 2026)**:
  - Contenedor dedicado `novadrive-redpanda` (`docker.redpanda.com/redpandadata/redpanda:latest`, modo dev de un solo nodo), puertos **29092** (Kafka) / **29644** (admin) — deliberadamente distintos de los que usa `andes-redpanda` (9092/9644, actualmente detenido en esta máquina) para que ambos puedan coexistir sin choque si algún día corren a la vez, aunque la decisión de fondo (#1) sigue siendo brokers completamente separados, no compartidos.
  - Los 3 tópicos de la sección 12 creados y verificados: `cdc.novadrive.customer`, `cdc.novadrive.vehicle`, `cdc.novadrive.sale` (1 partición cada uno, MVP).
  - **Hallazgo real (mismo patrón de la lección de Andes sobre "redirect a la dirección anunciada", pero encontrado en una herramienta distinta)**: `rpk topic create` ejecutado con `docker exec` (o sea, *desde dentro* del contenedor) fallaba con `connection refused` al dirección `127.0.0.1:29092` — porque esa es la dirección **anunciada para el host**, y desde dentro del propio contenedor `127.0.0.1:29092` no existe (el contenedor solo expone `9092` internamente). La solución no fue tocar el broker: se creó los tópicos con el mismo cliente Python (`confluent-kafka` `AdminClient`) que van a usar el worker y el bridge, corrido **desde el host** contra `127.0.0.1:29092` — funcionó a la primera y de paso validó el mismo camino de conexión que usará el código real.
  - Hallazgo menor de entorno: `MSYS_NO_PATHCONV` en Git Bash (Windows) necesita estar activo para `docker exec` pero DESACTIVADO para `docker cp` en el mismo flujo — ya lo veníamos aplicando puntual por comando desde la Fase 1, se repitió acá sin sorpresas.
- **Fase 6a — Worker de outbox (`worker/outbox_worker.py`), verificado con evidencia real de extremo a extremo (9 sep 2026)**:
  - Reclamo de lote con la consulta única `SELECT ... FOR UPDATE SKIP LOCKED LIMIT n` — la misma que se verificó con concurrencia real en la Fase 1, sin portar el patrón de dos pasos de Oracle/Andes (confirmado que no hacía falta).
  - **Corrida real contra los 39 eventos PENDING acumulados por los checkpoints de los tres microservicios** (12 customer + 20 vehicle + 7 sale): el worker los publicó todos en 2 ciclos (batch=20), cada uno con **topic/partición/offset reales logueados** (ej. `PUBLISHED event_id=... topic=cdc.novadrive.sale partition=0 offset=4`).
  - **Verificado por dos canales independientes, no solo el log del worker**: (1) Postgres — `SELECT status, count(*) FROM nd.outbox_event GROUP BY status` → los 39 en `PUBLISHED`; (2) un **consumidor Kafka real** leyendo directo de Redpanda (`get_watermark_offsets` + lectura de mensajes reales) confirmó **39 mensajes reales en el broker** repartidos exactamente igual por tópico (12/20/7), y una muestra de payload real coincide byte a byte con lo que generó `common/events/serialization.py`.
- **Fase 6b — Bridge Redpanda→Databricks Bronze (`bridge/`), verificado con evidencia real de extremo a extremo (9 sep 2026)** — ⚠️ **la decisión de arquitectura de esta fase quedó SUPERSEDIDA en la Fase 14 (INSERT SQL → Volume + `.jsonl`, ver ahí y hallazgo #7/#17)**; se conserva el resto tal cual para trazabilidad histórica (la tabla `bronze.events` mencionada abajo ya no se usa):
  - **Decisión de diseño marcada explícitamente (no asumida en silencio)**: a diferencia de Andes (JSONL a un Volume + Auto Loader `availableNow` + Job de Databricks), el bridge de NovaDrive hace **INSERT directo por lote** a `novadrive_catalog.bronze.events` vía la Statement Execution API, desde el mismo proceso Python continuo (`loop_continuous()`, patrón idéntico a `worker/outbox_worker.py::loop()`). Razón: Andes necesita las garantías de checkpointing/exactly-once de Auto Loader porque su Silver depende de eso; NovaDrive no tiene Silver (fuera de alcance) y el propio PDF permite duplicados en Bronze ante reintento — así que el proceso Python persistente ya cumple la regla "no lanzar un Job de Databricks por cada POST; mantener un consumidor continuo" sin necesitar Volume, Auto Loader ni Job en absoluto. Mucha menos infraestructura para el mismo requisito real.
  - **Catálogo propio creado y verificado** (`databricks/sql/ddl_bronze.sql`): `novadrive_catalog` (nuevo), schema `novadrive_catalog.bronze` únicamente (sin silver/gold/quarantine, ver `## Alcance recortado`), tabla `novadrive_catalog.bronze.events` con las mismas 7 columnas que `andes_catalog.bronze.eventos` (mismo contrato Bronze entre los dos sistemas, cada uno en su propio catálogo) — verificado con `SHOW TABLES`/`DESCRIBE TABLE` reales, no solo el mensaje de éxito del DDL.
  - **Bug real encontrado y corregido al aplicar el DDL**: el script que partía el archivo `.sql` por `;` descartaba una sentencia completa (`CREATE CATALOG...`) porque su primera línea era un comentario `--` y el filtro comprobaba `statement.strip().startswith('--')` sobre el bloque entero en vez de línea por línea — la sentencia real quedaba escondida detrás del comentario y se perdía en silencio. Notado porque `CREATE SCHEMA` falló con `NO_SUCH_CATALOG_EXCEPTION` en vez de tener éxito; corregido filtrando comentario por línea antes de unir cada sentencia, reverificado con las 3 sentencias reales aplicadas (`SUCCEEDED` las 3).
  - Mismo workspace Databricks que Andes (credenciales reutilizadas — mismo `DATABRICKS_HOST`/`DATABRICKS_TOKEN`, mismo `DATABRICKS_WAREHOUSE_ID` — ver `## Infraestructura y ubicaciones`), catálogo separado. **No se creó ningún Job de Databricks para NovaDrive** — a propósito, ver decisión de diseño arriba.
  - **Trazabilidad de extremo a extremo verificada con un solo `correlation_id` real, las 4 capas** (satisface el criterio de finalización de la sección 24 del PDF): `POST /customers` real → fila `PENDING` real en `nd.outbox_event` (mismo `event_id`) → un ciclo real del worker → `PUBLISHED` real con `topic=cdc.novadrive.customer partition=0 offset=12` → una corrida real del bridge → query real a Bronze con el **mismo `event_id` y `correlation_id` intactos** en el payload. Los cuatro pasos con su propia query/log real, no inferidos.
  - `bridge/_env.py` replica el helper de carga de `.env` de Andes (mismo motivo: nunca pedirle a un humano que exporte un secreto a mano).
- **Nota de transparencia (seguridad operativa, no oculto)**: al copiar `DATABRICKS_HOST`/`DATABRICKS_TOKEN` desde `andes-motors/.env` hacia el `.env` de NovaDrive (mismo criterio de reutilizar el mismo workspace, decisión #7), el harness mostró el valor del token en texto plano en una notificación automática de "archivo cambiado en disco" — no fue pegado por un humano ni impreso a propósito, pero el token quedó expuesto en este transcript igual. `andes-motors/.env` y `novadrive/.env` siguen gitignored en ambos repos, y el token no se imprimió en ningún otro punto de esta sesión. Queda en manos de CJ decidir si rotarlo por precaución.
- **Worker en modo loop continuo, verificado reaccionando solo, sin disparo manual** (9 sep 2026): `worker/outbox_worker.py::loop()` corriendo de fondo — un `POST /customers` real disparó un `PUBLISHED` real con topic/partición/offset en el log **~13 segundos después, sin que nadie llamara a `run_cycle()`** (el poll de 1s lo recogió solo). Es la réplica única que exige la sección 2/13 del PDF.
- **Fase 7 — Dockerfiles y despliegue, verificados de extremo a extremo con evidencia real, incluidos 3 bugs reales encontrados y corregidos en el camino (9 sep 2026)**:
  - **Una sola imagen, 5 entrypoints** (`Dockerfile` + `docker-entrypoint.sh`), tal como estaba propuesto: `customers`/`inventory`/`orders`/`worker`/`bridge` seleccionados por el primer argumento del contenedor. Sin `build-essential`/`librdkafka-dev` — confirmado que `confluent-kafka` instala desde wheel prebuilt en `python:3.12-slim` (x86_64) sin compilar nada, cerrando en la práctica el riesgo que Andes dejó "nunca verificado" en su propia lección transversal.
  - **`docker-compose.yml`** (despliegue completo: Postgres + Redpanda propios + los 5 procesos) — a diferencia del smoke test manual de esta sesión (que usó `host.docker.internal`, un nombre mágico solo de Docker Desktop Windows/Mac), los contenedores se hablan entre sí por **nombre de servicio dentro de la red de compose** (`redpanda:9092`, `postgres:5432`) — portable a un VPS Linux real, no atado a esta máquina. Puertos de host parametrizados vía `.env` (`CUSTOMERS_PORT`, `INVENTORY_PORT`, `ORDERS_PORT`, `REDPANDA_EXTERNAL_PORT`, etc.) para poder levantar una pila aislada sin chocar con otra ya corriendo (ver verificación abajo).
  - **Redpanda con dos listeners desde el compose** (`INTERNAL://redpanda:9092` para contenedores, `EXTERNAL://127.0.0.1:<puerto>` para herramientas del host) — la sección 4 de `LECCIONES_TRANSVERSALES.md` lo advertía como riesgo teórico; acá se confirmó como **error real, no hipotético**: el primer contenedor de prueba (`docker run` suelto, un solo listener) falló con `Connection refused` al intentar reconectar a `127.0.0.1:29092` desde dentro de sí mismo — la dirección anunciada solo tiene sentido para el host. Resuelto con el segundo listener antes de escribir el compose definitivo.
  - **Bug real #1 — `rpk cluster health --brokers ...` no existe** (ese subcomando no acepta `--brokers`, usa el perfil local de rpk dentro del propio contenedor): el healthcheck de Redpanda en el primer borrador de `docker-compose.yml` fallaba con `unknown flag`, bloqueando el arranque de todo lo que dependía de él (`depends_on: condition: service_healthy`). Diagnosticado ejecutando el comando a mano dentro del contenedor (`docker exec ... rpk cluster health`) antes de asumir que el broker estaba mal configurado — no lo estaba. Corregido a `rpk cluster health` a secas.
  - **Bug real #2 — filas del outbox atascadas en `PROCESSING` para siempre si el worker muere a mitad de ciclo** (encontrado con un caso real, no teórico): un contenedor de prueba fue destruido (`docker rm -f`) justo después de reclamar una fila (`claim_batch` ya había hecho `commit()` marcándola `PROCESSING`) pero antes de registrar el resultado de la publicación — la fila quedó huérfana, porque `claim_batch()` solo reclama `PENDING`/`RETRY`, nunca `PROCESSING`. **Fix de dos capas**: (1) *visibility timeout* — `claim_batch()` ahora también reclama filas `PROCESSING` cuyo `claimed_at` supera `STALE_PROCESSING_SEC` (default 120s, nuevo setting en `common/config.py`); (2) defensa en profundidad — `run_cycle()` ahora envuelve `publish_one()` en `try/except`, así un fallo síncrono del cliente Kafka (ej. `BufferError`) marca `RETRY` de inmediato en vez de depender solo del timeout. **Reverificado con el caso real**: la fila huérfana (`bee712b4...`, 2 intentos previos) fue reclamada de nuevo y publicada con éxito apenas pasó el umbral (bajado a 5s para la prueba), confirmado por `claimed_at`/`published_at` reales en Postgres.
  - **Verificación de extremo a extremo del `docker-compose.yml` completo, en una pila aislada** (proyecto `novadrive-deploytest`, puertos y volumen propios, sin tocar el entorno "vivo" de esta sesión): `postgres`+`redpanda`+`customers`+`inventory`+`orders`+`worker` levantados con `depends_on`/`service_healthy` respetando el orden real; **health real 200 en los 3 servicios web**; `POST /customers` real → el worker containerizado (conectado a Redpanda por `redpanda:9092`, DNS interno de compose, no `host.docker.internal`) lo publicó solo, log real con topic/partición/offset; confirmado con query real a la Postgres del propio compose. Pila desmantelada al terminar (`down -v`) — era solo para verificar el artefacto de despliegue, no queda corriendo en paralelo al entorno vivo. El entorno vivo (venv + `novadrive-postgres-dev` + `novadrive-redpanda`) se confirmó intacto después (health 200 en los 3 servicios, `pytest` 32/32).
  - **`bridge` definido en el compose pero deliberadamente NO levantado ni probado en este corte**: depende de `DATABRICKS_TOKEN`, que CJ está rotando en este mismo momento (ver nota de transparencia de más abajo) — no tiene sentido probarlo con una credencial que está a punto de revocarse. Queda pendiente para el checkpoint siguiente, apenas confirme el valor nuevo en ambos `.env`.
  - `docker-compose.local.yml` (Postgres solo, Fase 1) se mantiene sin cambios para el caso de uso mínimo de desarrollo; `docker-compose.yml` es el nuevo artefacto de referencia para el despliegue completo.
- **Fase 8 — Token de Databricks rotado, bridge verificado en el compose completo, y guía de arranque (`RUNBOOK.md`) validada palabra por palabra (9 sep 2026)**:
  - CJ rotó **solo** el token de NovaDrive (`novadrive/.env`) — confirmado presente, con prefijo `dapi`, longitud correcta y distinto del anterior, **sin imprimirlo nunca** (verificación por `grep`/longitud/sufijo, no por lectura directa del archivo). `andes-motors/.env` explícitamente NO se tocó, a pedido — queda para cuando CJ retome ese proyecto. **El token nuevo es de un workspace/cuenta personal de Databricks, no de una cuenta del equipo — es una solución temporal para poder seguir probando, no la credencial definitiva.** Reemplazarlo por una cuenta compartida del equipo es un pendiente explícito (ver más abajo), bloqueado hasta que CJ consiga ese host/token.
  - **Pila completa (`docker-compose.yml`, los 7 contenedores incluido `bridge`) verificada de extremo a extremo con el token nuevo**: se detuvo el entorno ad hoc de desarrollo (venv + contenedores sueltos `novadrive-postgres-dev`/`novadrive-redpanda` de las fases anteriores) para levantar todo desde el compose real — el volumen de Postgres se conservó (mismo nombre de proyecto por defecto, los 47 eventos históricos seguían ahí). `POST` real de customer + inventory + **order** (transacción cruzada completa) → worker containerizado publicó los 4 eventos con topic/partición/offset reales → bridge containerizado los subió a Bronze → **query real contra Databricks confirmando el mismo `correlation_id`** de la orden, con el token nuevo. Pila desmontada limpia al terminar (`down`, sin `-v`, conservando datos).
  - **Dos hallazgos reales al armar el checkpoint, corregidos antes de dárselos a CJ**: (1) `scripts/` no estaba copiado en la imagen Docker — `python -m scripts.create_topics` fallaba con `ModuleNotFoundError` dentro del contenedor; agregado al `Dockerfile`. (2) La consulta a Bronze del primer borrador apuntaba al contenedor `customers`, que no tiene `DATABRICKS_HOST`/`DATABRICKS_TOKEN` en su entorno (solo `bridge` los tiene) — corregido a `docker compose exec bridge ...`.
  - **`RUNBOOK.md`** (gitignored, mismo criterio que Andes) — guía de 9 pasos numerados, en español, con "qué deberías ver" en cada uno y una sección de solución de problemas. **Ejecutada literalmente, paso por paso, desde cero absoluto** (`down -v` primero, para probar el camino más exigente: sin datos, sin tópicos) antes de entregarla — no se le pidió a CJ que sea el primero en probarla. Encontrado y corregido en el camino: el VIN de ejemplo del Paso 5 (`1TESTVIN000000001`) tenía una `I`, inválida según la propia validación de charset VIN de NovaDrive — cambiado a `1TESTVN0000000001` y reverificado. Los 9 pasos, con las correcciones, se corrieron de punta a punta con resultados reales idénticos a los documentados en la guía.
  - `scripts/create_topics.py` nuevo (idempotente, crea los 3 tópicos si no existen) — necesario porque Redpanda no tiene volumen persistente en el compose (a diferencia de Postgres), así que cada contenedor nuevo empieza sin tópicos.
- **Los 6 componentes del alcance recortado (3 microservicios + outbox + worker + bridge a Bronze) están completos, contenerizados y verificados de punta a punta — incluido el despliegue local reproducible con una guía probada literalmente.**
- **Sesión de pruebas manuales de CJ (9 sep 2026)**: pila completa (`docker-compose.yml`) levantada a pedido para que CJ probara los 3 microservicios con sus propias manos vía navegador (`admin`/`changeme`), siguiendo `RUNBOOK.md`. Se recrearon los tópicos de Redpanda (sin volumen persistente, se pierden en cada `docker compose down`+`up` completo) y se confirmó que los datos de referencia (`nd.branch`/`nd.sales_agent`) seguían en el volumen de Postgres de sesiones anteriores.
- **Fase 9 — Simuladores en los 3 formularios, completa y verificada con Playwright real (9 sep 2026)**: ver detalle en el pendiente #1 (abajo), marcado completo. Commit `19b9297` en `novadrive-microservices`.
- **Fase 10 — Bridge: fin del reintento silencioso + `DATABRICKS_WAREHOUSE_ID` sin hardcodear, verificado con fallo real forzado (9 sep 2026)**: ver hallazgos técnicos #11/#12/#13 para el detalle completo (causa, fix, evidencia). En resumen: `enable.auto.commit=False` + commit manual solo tras flush exitoso (el auto-commit anterior podía dar por consumido un mensaje que nunca llegó a Bronze); umbral de `MAX_CONSECUTIVE_FAILURES` (default 8) que escala a `CRITICAL` y termina el proceso a propósito; `restart: unless-stopped` nuevo en `bridge` (docker-compose.yml) para que Docker lo reinicie solo, visible en `docker ps`/`docker logs`; `MAX_BUFFER_SIZE` (default 2000) con backpressure (deja de hacer `poll()`) en vez de crecer sin límite; `DATABRICKS_WAREHOUSE_ID` ya no tiene default hardcodeado al warehouse de Andes, ahora vive explícito en `.env`. Verificado con un fallo real forzado (`DATABRICKS_WAREHOUSE_ID` inválido a propósito, umbral bajado a 2 para la prueba): 3 ciclos completos de crash-loop reales (`RestartCount` real subiendo, logs `1/2`→`2/2`→`CRITICAL` reales), el evento pendiente nunca creció ni desapareció del buffer en ninguno de los 3 reinicios, y al restaurar la config correcta una query real contra `novadrive_catalog.bronze.events` confirmó que llegó **exactamente una vez** (`COUNT=1`). Commit `41a2bad` en `novadrive-microservices`.
- **Fase 11 — Reverse proxy (Caddy), completa y verificada con Playwright real (9 sep 2026)**: ver detalle en el pendiente #2 (abajo), marcado completo.
- **Fase 12 — Despliegue en el VPS de DigitalOcean, completo y verificado con evidencia real de extremo a extremo (9 sep 2026)**: ver detalle completo en el pendiente #3 (abajo), marcado completo.
- **Fase 13 — Datos de referencia en producción + fix de formato de precio, verificado con Playwright real contra la URL pública (9 sep 2026)**:
  - `scripts/seed_reference_data.py` corrido contra `docker-compose.prod.yml` (`docker compose run --rm --entrypoint python customers -m scripts.seed_reference_data`, mismo patrón que `create_topics.py` en Fase 8) — sin esto, el simulador de Inventory no tenía sedes para elegir y, en cascada, el de Order no tenía inventory disponible. **Verificado con query real** contra Postgres de producción: 3 branches (`NovaDrive Downtown`/`Uptown` en Metropolis, `NovaDrive Riverside` en Rivertown) y 4 agents (Jordan Vega, Casey Nguyen, Morgan Blake, Riley Ortiz), nombres/ciudades realistas ya presentes en el script, no inventados para esta corrida.
  - **Simuladores de Inventory y Order verificados con Playwright real contra `http://167.71.18.55`** (post-siembra): Inventory creó una unidad real (Mazda CX-5), Order creó una orden real (`ORD-20260909-152307-7FF4`) emparejando un customer y una unit reales — confirmado por conteo de filas antes/después en ambos casos, no solo por el código HTTP.
  - **Bug de formato de precio (`"12.229.00"`, punto como separador de miles Y decimal a la vez) — investigado a fondo antes de tocar código**: grep exhaustivo de todo el repo (Jinja `format`, JS `toLocaleString`/`toFixed`, cualquier lógica de agrupación) → cero resultados, ningún código propio agrega separador de miles. Reproducción real con Playwright (DOM `input.value` + screenshot) contra la URL pública, forzando `locale: es-EC` en el navegador → el valor crudo se mantuvo plano (`32999.99`) en `en-US` y en `es-EC` por igual — **no se pudo reproducir el glitch exacto con Playwright headless**, porque ese `locale` de Playwright solo cambia `navigator.language`, no el locale ICU real del sistema operativo que consulta el spin-input **nativo** de `<input type="number">` de un navegador real. **Diagnóstico**: es ese renderizado nativo (dependiente del locale del SO de quien mira la pantalla, no del código) el que puede agrupar miles con `.` sobre un valor que ya usa `.` como separador decimal, produciendo el resultado inválido — no reproducible por script, pero consistente con lo reportado. **Fix que elimina la clase de bug entera, no un parche puntual para un caso**: los 5 campos de dinero/porcentaje (`list_amount` en Inventory; `unit_gross_amount`/`discount_pct`/`tax_amount`/`captured_amount` en Order) pasan de `type="number"` a `type="text"` + `inputmode="decimal"` + `pattern` de validación — sin la capa de render nativa dependiente del SO, el formato mostrado es siempre el mismo sin importar la máquina de quien lo mire; `inputmode="decimal"` conserva el teclado numérico en móvil. Campos enteros sin este riesgo (`model_year`, cantidad/intervalo de los simuladores) se dejaron como `type="number"`, sin cambios. **Verificado con Playwright real contra producción**: el input de `list_amount` quedó `type="text"`/`inputmode="decimal"`, con el valor correcto (`32999.99`); `checkValidity()` real confirma que el `pattern` **rechaza** `"12.229.00"` (`false`) y **acepta** `"32999.99"` (`true`). La validación de negativos/rango en servidor (Pydantic) no cambió — sigue siendo la garantía real, el `pattern` del lado cliente es solo una ayuda de UX adicional.
- **Fase 14 — Bridge migrado de INSERT SQL directo a Volume + `.jsonl` (mismo patrón que Andes), completo y verificado con evidencia real en ambos entornos (9-10 sep 2026)**: revierte la decisión de la Fase 6b/hallazgo #7 (ver ambos, marcados `SUPERSEDIDA`) — pedido explícito del equipo de trabajo, requisito no negociable de consistencia entre proyectos hermanos, confirmado por CJ después de que se le señalara la contradicción con la decisión anterior (no aplicado en silencio).
  - Migración de cuenta de Databricks **número 2 en el día** (workspace nuevo, `identity` confirmada real vía `/api/2.0/preview/scim/v2/Me` antes de asumir nada — dos intentos previos de `WAREHOUSE_ID` en sesiones anteriores de este mismo día fallaron con `404` real antes de dar con el correcto, ver hallazgo #16).
  - `novadrive_catalog` ya existía en el workspace nuevo (creado antes por el compañero de análisis de datos, `owner` distinto al mío) — schema `bronze` y Volume `raw_events` creados por mí, **vía Unity Catalog REST API, sin arrancar ningún SQL Warehouse en ningún momento** (confirmado con el warehouse en `STOPPED` durante toda la operación) — ver hallazgo técnico #17.
  - `bridge/databricks_files.py` nuevo (helper de `PUT` crudo a la Files API) + `bridge/bridge_redpanda_to_databricks.py::upload_batch()` reescrito: ya no arma `INSERT`, arma un archivo `.jsonl` (un evento por línea, mismo sobre que Andes: `kafka_topic`/`partition`/`offset`/`kafka_timestamp` + `ingested_at` + `payload` + `source_file`) y lo sube a `novadrive_catalog.bronze.raw_events`. Nombre `novadrive_batch_<fecha>_<hora>_<microsegundos>.jsonl` (mismo formato que Andes, prefijo distinto). **Se conservan a propósito** las protecciones propias de NovaDrive (hallazgos #11/#12: `enable.auto.commit=False` + commit solo tras subida exitosa, umbral `MAX_CONSECUTIVE_FAILURES`/backpressure `MAX_BUFFER_SIZE`) — son ortogonales a "SQL vs archivos", Andes no las tiene, y no se pidió quitarlas.
  - **Verificado con evidencia real, en ambos entornos por separado** (local y VPS de producción): rebuild + recreate de `bridge` en cada uno; un `POST /customers` real en cada entorno → evento `PUBLISHED` real → **archivo `.jsonl` real confirmado por 3 vías independientes**: log del bridge, `GET /api/2.0/fs/directories` (listado real del Volume con tamaño de archivo), y contenido real descargado y leído (no solo el código de estado del `PUT`) — coincide exactamente con el evento creado, mismo `event_id`/`correlation_id`.
  - **Pendiente explícito, NO resuelto todavía dentro de esta fase**: la tabla SQL `novadrive_catalog.bronze.events` de la migración anterior (workspace previo, `dbc-1814bab8-c629...`) no se pudo eliminar — esas credenciales ya no están disponibles (se perdieron al sobrescribir el `.env` con las de este workspace nuevo, por diseño no se guardan aparte). Queda pendiente que CJ decida: reproveer esas credenciales para borrarla, o confirmar que se abandona ahí sin borrar.
- **Fase 15 — Split de Volume por tipo de evento (mismo patrón que Andes), completo y verificado con evidencia real en ambos entornos (10 sep 2026)**: replica en NovaDrive el mismo split que se hizo en Andes — en vez de un solo Volume genérico con todo mezclado, cada batch se agrupa por el campo `entity` del propio payload y sube un archivo `.jsonl` por grupo al Volume específico de esa entidad.
  - **3 Volumes nuevos** (`novadrive_catalog.bronze.raw_events_customer/vehicle/sale`) creados vía Unity Catalog REST API, **sin arrancar ningún SQL Warehouse** (mismo camino que Fase 14/hallazgo #17) — verificado con `GET` real listando los 4 Volumes (3 nuevos + `raw_events` original intacto, sin tocar ni renombrar). Todo dentro de `novadrive_catalog` — `andes_catalog` no se leyó ni se tocó en ningún momento.
  - `group_by_volume()`/`_entity_for()` nuevas en `bridge/bridge_redpanda_to_databricks.py`: función pura en memoria (sin red), separada de `upload_batch()` a propósito para poder probarla aislada. Un mensaje con `entity` desconocida, faltante, o un payload que ni siquiera es JSON válido cae al Volume genérico original (`raw_events`, sin sufijo) — nunca se descarta en silencio. `upload_batch()` ahora sube **un archivo por grupo** (puede ser más de uno por ciclo si el batch tenía mezcla de tipos) y devuelve la lista de rutas subidas.
  - **`tests/test_bridge_grouping.py` nuevo, 8 casos, corridos ANTES de tocar red o producción** (mismo criterio pedido: probar la lógica de agrupamiento mockeada primero) — incluye entity desconocida, faltante, payload corrupto, payload JSON que no es un objeto, batch mixto (ningún mensaje se pierde), batch vacío. **40/40 tests del proyecto siguen pasando** tras el cambio.
  - **Decisión explícita, documentada en el propio código**: no se agregó un `try/except` por subida individual dentro de `upload_batch()` (a diferencia de lo que sí necesitaría el bridge de Andes) — el `try/except` de `loop_continuous()` ya cubre cualquier fallo de subida con el umbral de reintentos/backpressure/`CRITICAL` que Andes no tiene en el suyo; agregar uno más sería protección duplicada.
  - **Verificado con evidencia real de extremo a extremo, en ambos entornos**: rebuild + recreate de **solo** `bridge` (sin tocar el resto de los servicios) en local y en el VPS. Un customer + un inventory unit + una order reales (la order generó `sale c` + `vehicle u` en el mismo flujo, el caso mixto mencionado en el pedido) → **cada uno de los 4 eventos terminó en su Volume correcto**, confirmado por 2 vías independientes cada vez: listado real del directorio (`GET /api/2.0/fs/directories`) y contenido real descargado y leído (`entity`/`event_id` coincidiendo exacto). Repetido también en local con un quinto evento.
  - **Registros de prueba limpiados después, respetando el orden de las relaciones**: orden eliminada primero (libera el inventory unit, genera `sale d` + `vehicle u` reales) → inventory unit eliminado → customer eliminado — los 3 vía los endpoints reales de la app (nunca DML manual). Los 9 archivos `.jsonl` de prueba en los 3 Volumes nuevos borrados vía `DELETE` real a la Files API, verificado con `GET` real que los 3 Volumes quedaron en 0 archivos.
  - **Nota de transparencia, no parte de esta fase**: el Volume genérico `raw_events` todavía tiene 5 archivos de prueba de la Fase 14 (antes de que existiera el split) — no se tocaron porque no eran parte del alcance de esta tarea; queda a criterio de CJ si limpiarlos en algún momento.
- **Fase 16 — Notebook de Bronze por tipo de evento (Auto Loader), entregable preparado pero NO ejecutado ni desplegado (10 sep 2026)**: replica en NovaDrive el notebook `01b_bronze_autoloader_por_tipo.py` que ya existe y funciona en Andes (mismo workspace compartido, `andes_catalog` — **no se leyó, escribió ni listó nada de ese catálogo en ningún momento de esta fase**, confirmado explícitamente por diseño de las llamadas hechas).
  - **Checklist de prerequisitos verificado con llamadas reales de solo lectura a la Unity Catalog REST API** (sin crear nada): catálogo `novadrive_catalog` ✅, schema `bronze` ✅, los 3 Volumes fuente (`raw_events_customer/vehicle/sale`) ✅ — los 4 confirmados reales. El Volume de estado `bronze_pipeline_state` confirmado que **NO existe** (`404` real), tal como se esperaba.
  - **`databricks/notebooks/01b_bronze_autoloader_por_tipo.py` nuevo** — mismo patrón exacto que el notebook real de Andes (leído primero para replicar estilo y lógica, no solo la idea): una función `procesar_bronze()` reusada 3 veces (no 3 bloques copiados), checkpoints/schema/bad_records en un Volume de estado separado de los Volumes fuente, `rescuedDataColumn` + `badRecordsPath` como dos redes de seguridad distintas, `maxFilesPerTrigger=1000`, tolerancia a fallo parcial con excepción a propósito al final si algún origen falló (para que el Job quede `Failed` sin revertir lo que sí salió bien), y `trigger(availableNow=True)` — nunca `ProcessingTime`/`Continuous` (el workspace es serverless-only).
  - **Diferencia real respecto al de Andes, documentada explícitamente en el propio notebook**: NovaDrive no tiene Silver (fuera de alcance), así que las tablas Bronze de destino usan el nombre de `entity` directo (`bronze.customer`/`bronze.vehicle`/`bronze.sale`), sin traducir a una convención de Silver que no existe. Además, a diferencia de Andes (que conserva un notebook viejo para su Volume genérico de respaldo), **este es el primer y único notebook de Auto Loader de NovaDrive** — el Volume genérico `raw_events` (respaldo para `entity` desconocida/corrupta) queda sin un notebook equivalente que lo materialice en tabla; asimetría real frente a Andes, anotada a propósito, no un descuido.
  - **Nada se subió al workspace de Databricks ni se ejecutó** — ni el notebook (solo vive en el repo), ni el Volume de estado, ni ningún Job — cumpliendo el pedido explícito de dejarlo como entregable para que el equipo decida cuándo correrlo y controle el consumo de crédito real. Verificado con evidencia real que el estado del workspace no cambió: los mismos 5 objetos de antes (catálogo/schema/3 Volumes fuente) siguen siendo los únicos existentes, `bronze_pipeline_state` sigue sin existir.
  - **Nota de honestidad heredada de Andes, no verificada en ningún proyecto todavía**: la combinación de `rescuedDataColumn` + `badRecordsPath` con Auto Loader no se probó en una corrida real — quien ejecute este notebook por primera vez debe confirmarlo con datos reales, no asumirlo de la documentación.
- **Fase 17 — Notebook de Silver por tipo de evento (homologación + MERGE + stubs + Quarantine), entregable preparado pero NO ejecutado ni desplegado (10 sep 2026)** — **ver la ampliación de alcance en `## Alcance recortado`, arriba, antes de leer esto**: Silver/Quarantine para NovaDrive estaban explícitamente fuera de alcance desde el 9 sep 2026; esta sesión señaló esa contradicción antes de escribir una sola línea, y CJ confirmó explícitamente que es una decisión consciente (trabajo del compañero de arquitectura medallón, ejecución manual, mismo criterio que Bronze).
  - **Confirmación real de que Bronze (Fase 16) ya corrió de verdad**: CJ pasó filas reales de `bronze.customer`/`vehicle`/`sale` con datos de hoy (10 sep 2026) — el notebook `01b` que se dejó como entregable en Fase 16 fue ejecutado por el equipo, tal como se esperaba. Confirmado también con llamadas reales a la API que las 3 tablas Bronze existen.
  - **`databricks/notebooks/02b_silver_merge_por_tipo.py` nuevo** — se leyó el notebook REAL y completo de Andes (`02b_silver_merge_por_tipo.py`, 613 líneas) antes de escribir este, para replicar estilo y arquitectura exactos, no solo la idea general. Mismo patrón: función `procesar_silver()` reusada 3 veces, dedup por `row_number()` dentro del micro-batch, `MERGE` idempotente (`whenMatchedUpdate`/`whenNotMatchedInsertAll`), Quarantine compartida, Volume de estado `silver_pipeline_state` separado del de Bronze, tolerancia a fallo parcial con excepción al final.
  - **Homologación reescrita entidad por entidad con los campos REALES de NovaDrive** (confirmados contra las filas de Bronze que pasó CJ, no inventados): `CUSTOMER_STATUS` A/S/X → `ACTIVE`/`SUSPENDED`/`CLOSED` (a diferencia de Andes, que solo tiene 2 estados); `AVAILABILITY_CODE` AVL/HOLD/SOLD → `AVAILABLE`/`HELD`/`SOLD`; `ORDER_STATUS` (OPEN/APPROVED/INVOICED/CANCELLED) se deja **sin mapear**, ya es canónico en inglés — a diferencia de Andes, que sí traduce el estado de venta. Validación de VIN y de `model_year` homologadas contra las reglas que la propia app de NovaDrive ya aplica (`services/inventory/schemas.py`: charset sin I/O/Q, rango 1900-2100) en vez de copiar las de Andes (año actual+1).
  - **El caso de stub más delicado, verificado que aplica igual y se manejó**: en NovaDrive, `sale` referencia **dos** entidades (`BUYER_CODE` → `customer`, `CHASSIS_NUMBER` → `vehicle`) — a diferencia de Andes, donde el ejemplo documentado también son dos, así que el patrón (hasta 2 stubs por venta, uno de cada tabla) se replicó tal cual sin necesitar generalizarlo más. **El fix de `pendiente_completar = true OR occurred_at >= t.occurred_at` se copió sin modificar** (hallazgo #10 de Andes) — no se reintrodujo el bug.
  - **Sin nombres de Silver que igualar** (documentado en el propio notebook): NovaDrive no tenía tablas Silver preexistentes, a diferencia de Andes — las tablas nuevas se llaman directamente `silver.customer`/`vehicle`/`sale` (nombre de `entity`), sin traducir a ninguna convención previa.
  - **`databricks/sql/ddl_silver.sql` nuevo** — `CREATE SCHEMA`/`CREATE TABLE IF NOT EXISTS` para `silver.customer`/`vehicle`/`sale` y `quarantine.events`, tipos de columna alineados con las columnas reales de Postgres de NovaDrive (`DECIMAL(16,2)` para montos, `DECIMAL(7,4)` para `discount_pct`, `DECIMAL(15,2)` para `list_amount`, `CHAR(3)` para `currency_code`).
  - **Checklist verificado con llamadas reales de solo lectura, nada creado**: `silver`/`quarantine` (schemas) confirmados que **NO existen** (`404` real cada uno); `silver_pipeline_state` (Volume) confirmado que **NO existe** (`404` real). Re-verificado explícitamente DESPUÉS de escribir el notebook que el estado del workspace no cambió — mismos 3 `404`, nada se creó de forma accidental.
  - **`andes_catalog` no se leyó, escribió ni listó en ningún momento de esta fase** — todas las llamadas hechas fueron explícitamente contra rutas de `novadrive_catalog`.
  - **Nada se subió al workspace ni se ejecutó** — mismo criterio que Bronze: entregable para que el compañero de arquitectura medallón lo aplique manualmente y controle el consumo de crédito real.
- **Fase 18 — Gold (vistas SQL puras sobre Silver), APLICADO REAL contra el workspace, no solo entregable (10 sep 2026)** — a diferencia de Bronze/Silver (notebooks, entregable sin ejecutar): Gold es SQL puro sin streaming/notebook, de bajo riesgo y reversible, y el pedido esta vez sí incluía guía operativa de costo (agrupar en una sesión de warehouse ya abierta, apagarlo al terminar) — se interpretó como pedido de ejecución real, a diferencia de los dos anteriores, y se aplicó.
  - **Verificación previa gratuita** (sin warehouse, Unity Catalog REST API): `silver.customer`/`vehicle`/`sale` y `quarantine.events` confirmados reales (el DDL de Silver ya estaba aplicado). Confirmado también con `GET` real que `bronze.branch`/`bronze.sales_agent` **no existen** — `ND_BRANCH`/`ND_SALES_AGENT` nunca tuvieron tópico propio (dato de referencia sembrado directo en Postgres, ver "Modelo relacional Postgres"), así que `branch_code`/`sales_agent_code` quedan como códigos crudos en Gold, no nombres legibles — mismo comportamiento que Andes, confirmado con evidencia, no asumido.
  - **El SQL Warehouse ya estaba `RUNNING`** (el compañero de arquitectura medallón lo tenía arriba depurando el error de sesión del notebook de Silver) — se reusó esa misma sesión en vez de arrancar una nueva (mismo criterio de costo pedido), y **no se apagó al terminar** para no cortarle su sesión activa a otra persona.
  - **Conteo real de Silver antes de construir Gold encima**: `customer=6`, `vehicle=4`, `sale=2`, `quarantine.events=0` — confirma que el pipeline de Silver sí procesó datos reales pese al error de sesión reportado (el crash fue en una corrida posterior/repetida, no antes de que el primer MERGE exitoso ocurriera).
  - **`databricks/sql/ddl_gold.sql` nuevo** aplicado sentencia por sentencia (8 sentencias: `CREATE SCHEMA gold` + 7 vistas — `sales_summary`, `sales_by_day`, `available_inventory`, `top_selling_models`, `customers_summary`, `sales_detail`, `pipeline_health`) — las 8 `SUCCEEDED` reales, y **cada vista verificada con un `SELECT` real después**, no solo el mensaje de éxito del `CREATE VIEW`.
  - **El patrón de stub, demostrado funcionando con datos reales de producción, no solo en teoría**: `gold.sales_detail` muestra `customer_name = "CUSTOMER PENDING SYNC"` en una venta real cuyo customer todavía no había llegado a Silver cuando esa venta se procesó; `gold.customers_summary`/`gold.pipeline_health` cuentan correctamente ese caso (`customers_pending_sync=1`, `pending_sync customer=1`) — el mecanismo completo (Bronze → Silver con stub → Gold reflejándolo) verificado de punta a punta con un caso real, no fabricado para la prueba.
  - **`andes_catalog` no se leyó, escribió ni listó en ningún momento** — confirmado por inspección de las 8 sentencias aplicadas, ninguna lo menciona.
- **El entorno de desarrollo de esta máquina (venv + `docker-compose.yml` local) y el VPS de producción son dos despliegues completamente independientes** — la pila local puede haber quedado corriendo o apagada según lo último que se haya hecho manualmente; la del VPS quedó corriendo con `restart: unless-stopped` en casi todos los servicios (ver Fase 12) y no depende de esta máquina para seguir viva. **La sesión nueva debe verificar el estado real de cada una por separado** (`docker ps` local; SSH + `docker compose -f docker-compose.prod.yml ps` en el VPS) antes de asumir cualquiera de las dos, no inferirlo de este documento.

## Pendientes explícitos (en este orden — no reordenar sin que CJ lo pida)

1. ✅ **COMPLETO (9 sep 2026) — Simuladores en los 3 formularios** (Customer, Inventory, Order) — mismo alcance simple que los de Andes: crean registros reales espaciados en el tiempo usando los mismos endpoints POST reales del formulario (nada se simula fuera del flujo real). El de Order tiene la misma restricción de diseño que el de Andes: consulta pares customer/inventory disponibles **en vivo** en cada ciclo vía `GET /orders/options` (endpoint nuevo, solo lectura), sin sistema de reservas; si un ciclo no encuentra un par disponible, lo salta sin error — esto es comportamiento esperado, no un bug. **Verificado con Playwright real** (1 ciclo por simulador, contra los servicios reales): los 3 crearon registros reales confirmados por conteo de filas antes/después, incluido el caso "sin disponibilidad → con disponibilidad" en Order (la unidad creada por el simulador de Inventory fue la que tomó el de Order en el ciclo siguiente). **Aprobado por CJ.** Commit `19b9297`.
2. ✅ **COMPLETO (9 sep 2026) — Reverse proxy (Caddy)** para resolver que el navegador pide credenciales HTTP Basic por separado en cada uno de los 3 microservicios — al vivir en puertos distintos (8001/8002/8003) eran orígenes distintos para el navegador, así que autenticarse en uno no cubría a los otros dos. `Caddyfile` nuevo (raíz del repo) + servicio `caddy` en `docker-compose.yml`, un solo puerto público (`PROXY_PORT`, default **8080**), ruteo por prefijo de path sin reescribirlo (`handle`, no `handle_path` — cada microservicio ya monta sus rutas bajo `/customers`/`/inventory`/`/orders`, así que el proxy solo necesita mandar cada prefijo al contenedor correcto tal cual). `GET /events/{id}/status` (idéntico en los 3 servicios, misma tabla `nd.outbox_event`) ruteado a `customers` arbitrariamente. `CUSTOMERS_URL`/`INVENTORY_URL`/`ORDERS_URL` (usadas para armar los links del nav) ahora las 3 apuntan al mismo origen del proxy (antes, cada una a su propio puerto directo) — así el nav navega dentro de un solo origen sin importar desde cuál de los 3 microservicios se sirvió la página. Puertos directos 8001/8002/8003 siguen publicados y funcionando (útiles para debugging/checkpoints), no se quitaron. **Verificado con Playwright real, contexto de navegador limpio (equivalente a incógnito, sin credenciales guardadas)**: primera visita sin credenciales → 401 real; con credenciales dadas una vez, navegando `/customers` → `/inventory` → `/orders` → `/customers` → **cero desafíos 401 adicionales** (si fueran orígenes distintos, cada uno habría pedido su propio 401 antes del 200 — no pasó); los 3 links del nav confirmados apuntando a `:8080`, ninguno a `:8001`/`:8002`/`:8003`. Verificado además un `POST /customers` real y un `GET /events/{id}/status` real, ambos a través del proxy, ambos con resultado real (customer creado, evento con `status=PUBLISHED`). No requiere dominio real para funcionar en local; para el VPS con dominio propio (pendiente #3), cambiar `:80` por el dominio en `Caddyfile` activa HTTPS automático sin tocar código de negocio (ver comentario en el propio archivo). Commit `366f1e4`.
3. ✅ **COMPLETO (9 sep 2026) — Despliegue en el VPS de DigitalOcean.** CJ pasó host/usuario/password (guardados en `NOVADRIVE_PROD_SSH_HOST/USER/PASSWORD` en `novadrive/.env`, mismo patrón que `ANDES_PROD_SSH_*`). Droplet propio, distinto al de Andes (Ubuntu 24.04.4 LTS, 2vCPU/4GB nominal → **3.8GB reales** confirmado con `free -h`, mismo hallazgo que Andes ya documentó para el suyo — 120GB disco, 114GB libres). **Diagnóstico real antes de instalar nada**: sin Docker, sin swap, `ufw` inactivo, `git`/`curl` ya presentes. **Provisionado**: Docker Engine 29.8.0 + Compose v5.5.1 (script oficial `get.docker.com`); swap de 2GB creado y persistido en `/etc/fstab` (mismo criterio que Andes); repo clonado en `/opt/novadrive` desde `novadrive-microservices` (GitHub, ya no un rsync/copia local).
   - **`docker-compose.prod.yml` nuevo** (separado de `docker-compose.yml`, mismo patrón que Andes con el suyo) — pedido explícito de CJ (9 sep 2026), más estricto que el de Andes porque ahora hay Caddy de por medio: `postgres`/`redpanda`/`customers`/`inventory`/`orders` **sin `ports:`** (nada expuesto al host, ni con `ufw` de por medio — dos capas de defensa, no una), `redpanda` con volumen persistente nuevo (`novadrive_redpanda_data`, a diferencia del compose de desarrollo que es efímero a propósito) y un solo listener (nadie fuera de la red interna le habla en producción). `caddy` es la **única** superficie pública (80/443, 443 ya publicado para cuando haya dominio real). `CUSTOMERS_URL`/`INVENTORY_URL`/`ORDERS_URL` arman los links del nav desde `PUBLIC_BASE_URL` (nuevo, `.env.production.example`), no desde `localhost:PROXY_PORT`.
   - **Hallazgo real encontrado y corregido en el primer intento de despliegue** (ver hallazgo técnico #14): `redpanda` con `mem_limit` igual a su `--memory` interno se caía en loop con `insufficient physical memory` — corregido a `mem_limit: 640m` (margen de ~128MB), reverificado con éxito.
   - **`ufw` configurado exactamente como pidió CJ, confirmado explícito con evidencia real, dos verificaciones independientes**: `ufw status verbose` muestra `Default: deny (incoming)` con **solo** `22/tcp`, `80/tcp`, `443/tcp` permitidos (IPv4 e IPv6); `ss -tlnp` en el propio VPS confirma que **nada más** está en `LISTEN` en una interfaz pública (ni Postgres 5432, ni Redpanda 9092, ni los microservicios en 8001/8002/8003 — ninguno de los tres aparece porque `docker-compose.prod.yml` nunca les publica `ports:`, la ausencia de `ufw` no habría bastado sola). Confirmado además desde **afuera** del VPS (esta máquina): `curl` a los puertos 8001/5432/9092 del VPS público → timeout real (`exit 28`, sin respuesta), mientras que `:80` responde real (401 sin credenciales, 200 con ellas).
   - **Extremo a extremo real contra la URL pública**: `POST http://<IP>/customers` real (a través de Caddy, con Basic Auth) → evento `PUBLISHED` real (`GET /events/{id}/status`) → **query real contra `novadrive_catalog.bronze.events`** confirmando el mismo `event_id`/`correlation_id` en Bronze. Las 4 capas, con su propia evidencia, igual que se exigió para el entorno local en Fase 6b.
   - **Credenciales de producción nuevas, nunca las de desarrollo** (mismo criterio que Andes documentó como buena práctica): `POSTGRES_PASSWORD` generado al azar (`secrets.token_hex(16)`, vive solo en el `.env` del VPS, permisos `600`, nunca impreso ni siquiera en este chat); `APP_USERNAME`/`APP_PASSWORD` nuevos generados y comunicados una sola vez a CJ (no repetidos en texto plano después de esa entrega).
   - **Pendiente explícito dentro de este punto, NO bloqueante**: sin dominio real todavía, así que el sitio corre en HTTP plano sobre la IP pública (`http://<IP>/customers`), sin HTTPS. El día que CJ tenga un dominio apuntando a esta IP, activar HTTPS automático es cambiar `:80` por ese dominio en `Caddyfile` y `PUBLIC_BASE_URL` en el `.env` del VPS — nada más, ya documentado en el propio `Caddyfile`.
4. ✅ **COMPLETO (9 sep 2026) — Migrado de la cuenta personal de Databricks a la cuenta de un compañero de análisis de datos.** Motivo real, no planeado: la cuenta personal se quedó sin crédito a mitad de sesión (`CREDIT_EXHAUSTED`, el SQL Warehouse dejó de poder arrancar — ver hallazgo abajo), lo cual apuró este pendiente antes de lo previsto.
   - **Verificación previa, sin asumir nada** (dos intentos de `DATABRICKS_WAREHOUSE_ID` fallaron antes del que funcionó — confirmado con `GET /api/2.0/sql/warehouses/{id}` real en cada caso, nunca asumido): el primer valor entregado tenía forma de Workspace ID, no de Warehouse ID (`404`); el segundo era un warehouse real pero de un workspace distinto al `DATABRICKS_HOST` entregado en ese momento (`404` también); recién con el host y el warehouse ID del tercer intento (una cuenta nueva que el compañero creó sin avisar) la API confirmó el warehouse real (`eef61fa6a0996e5e`, "Serverless Starter Warehouse", `2X-Small`).
   - DDL aplicado contra el workspace nuevo (`CREATE CATALOG/SCHEMA/TABLE IF NOT EXISTS`, mismo esquema de `novadrive_catalog.bronze.events` de siempre) y **verificado con `SHOW TABLES`/`DESCRIBE TABLE` reales**, no solo el mensaje de éxito.
   - `.env` actualizado en **dos lugares**: local (esta máquina) y el del VPS de producción — en el del VPS, por **SFTP con reemplazo dirigido de solo las 3 líneas `DATABRICKS_HOST`/`DATABRICKS_TOKEN`/`DATABRICKS_WAREHOUSE_ID`** (nunca un SCP del archivo completo, para no pisar `POSTGRES_PASSWORD`/`APP_PASSWORD`/`PUBLIC_BASE_URL` — secretos propios de esa instancia, generados aparte). `DATABRICKS_CATALOG` se mantuvo igual (`novadrive_catalog`) — mismo nombre de catálogo, workspace distinto.
   - `bridge` recreado en ambos entornos (sin rebuild de imagen, solo `docker compose up -d bridge` — cambio de env, no de código).
   - **Verificado con evidencia real de extremo a extremo en AMBOS entornos por separado**: un `POST /customers` real (local vía `127.0.0.1:8001`, producción vía `http://167.71.18.55`) → evento `PUBLISHED` real → **query real contra el catálogo NUEVO** confirmando el mismo `event_id` en cada caso.
   - **El Bronze acumulado en la cuenta personal anterior (todo el de hoy — Fases 6b a 13, incluidas las pruebas del VPS) se queda intacto ahí, sin migrar ni borrar** — decisión explícita de CJ, no un efecto accidental. Queda separado del catálogo de la cuenta de equipo; cualquier análisis que necesite ese historial de hoy tiene que consultarse contra la cuenta vieja, no aparece en la nueva.

## Infraestructura y ubicaciones

- **Repositorio**: `novadrive-microservices` (repo separado, mismo criterio que `andes-motors-fastapi` para Andes — no es una carpeta dentro de un repo compartido).
- **`docker-compose.yml`** (raíz del repo) — pila completa de despliegue local: `postgres` (Postgres 16, puerto host parametrizable, default `15432` en este entorno de desarrollo por el conflicto documentado en `## Hallazgos técnicos no obvios` #1), `redpanda` (dos listeners, puerto externo parametrizable, default `29092`/`29644`), `customers`/`inventory`/`orders` (puertos `8001`/`8002`/`8003`, parametrizables vía `.env`), `worker` (una réplica, sin puerto expuesto), `bridge` (sin puerto expuesto, único servicio con `DATABRICKS_HOST`/`DATABRICKS_TOKEN` en su entorno). Todos desde la misma imagen (`Dockerfile` + `docker-entrypoint.sh`), seleccionados por el primer argumento del contenedor. `docker-compose.local.yml` es el compose mínimo (solo Postgres) de la Fase 1, se mantiene sin cambios para desarrollo puntual de modelos.
- **`RUNBOOK.md`** (raíz del repo, gitignored — mismo criterio que el de Andes, nunca commitear un archivo con pasos que puedan llevar a pegar un secreto). Guía de 9 pasos numerados para levantar la pila completa y probar los 3 microservicios de punta a punta hasta confirmar la llegada a Bronze, con sección de solución de problemas. **Ejecutada literalmente por esta misma sesión antes de entregarla a CJ** (ver Fase 8) — no es un documento aspiracional, cada paso tiene un resultado real verificado detrás.
- **Databricks — YA NO comparte workspace con Andes, y desde la Fase 14 tampoco comparte el patrón de ingesta con la Fase 6b/12** (workspace cambiado dos veces el 9-10 sep 2026, pendientes #4 y arquitectura de bridge ambos completos): NovaDrive vive ahora en un **tercer** workspace del día (`DATABRICKS_HOST`/`DATABRICKS_TOKEN`/`DATABRICKS_WAREHOUSE_ID` propios en `novadrive/.env`, credenciales de las 2 migraciones anteriores ya no disponibles — se pierden al sobrescribir el `.env`, por diseño). Andes sigue en su propio workspace de siempre (`dbc-5becafae-e891.cloud.databricks.com`) — **no se tocó nada de Andes en ninguna de las migraciones de hoy**, solo el `.env` de NovaDrive (local y VPS). **Ya NO hay tabla SQL de Bronze** — el catálogo `novadrive_catalog` (creado antes por el compañero de análisis de datos, no por esta sesión) tiene schema `bronze` con un **Volume** `raw_events` (`novadrive_catalog.bronze.raw_events`), no una tabla Delta — los eventos llegan como archivos `.jsonl` crudos, mismo patrón que Andes (Fase 14). Convertir esos archivos en tabla es un paso posterior, fuera de alcance salvo pedido explícito. **El Bronze de las 2 iteraciones anteriores (tabla SQL en la cuenta personal hasta Fase 12, y en el segundo workspace hasta Fase 14) quedó atrás sin migrar** — para consultar cualquiera de esos hay que recuperar esas credenciales, ninguna aparece en el Volume nuevo. La tabla SQL de la migración inmediatamente anterior a esta (workspace `dbc-1814bab8-c629...`) sigue sin eliminarse — pendiente explícito, ver Fase 14. Ver hallazgos técnicos #15/#16/#17.
- **VPS de producción** (DigitalOcean, pendiente #3 completo — ver Fase 12): droplet propio de NovaDrive (distinto al de Andes), Ubuntu 24.04.4 LTS, `167.71.18.55`. Acceso SSH por password (`NOVADRIVE_PROD_SSH_HOST/USER/PASSWORD` en `novadrive/.env`, nunca commiteado). Código en `/opt/novadrive` (clon real de `novadrive-microservices`, no una copia manual). `docker-compose.prod.yml` + `.env` real (permisos `600`) ahí — variables reales documentadas sin valores en `.env.production.example`. Login de la app: `APP_USERNAME`/`APP_PASSWORD` de producción, generados el 9 sep 2026, entregados a CJ por fuera de este documento (no se repiten en texto plano acá). URL pública: `http://167.71.18.55/customers` (sin dominio ni HTTPS todavía, ver nota en el pendiente #3). `ufw`: solo 22/80/443 permitidos, confirmado con evidencia real.
- **`andes-motors/.env` no se toca desde esta sesión de NovaDrive bajo ninguna circunstancia, salvo que CJ lo pida explícitamente por escrito en la conversación.** Ya se confirmó una vez que ese archivo se administra aparte, cuando CJ retome el proyecto Andes — no asumir que un cambio "de rutina" (rotar un token compartido, actualizar una URL) amerita tocarlo sin permiso nuevo.

## Referencias cruzadas

- `LECCIONES_TRANSVERSALES.md` — lecciones técnicas de construir Andes (outbox, Redpanda, Databricks, despliegue, seguridad). Es referencia para no repetir errores ya resueltos, no una lista de tareas — las partes de Silver/Gold/Job continuo ahí escritas no aplican a este proyecto (Bronze es el límite acá), el resto sí.
- Contrato de eventos compartido con Andes: mismo formato, mismas capas técnicas de outbox — pero cada sistema publica a su propio Redpanda y sube a su propio catálogo Bronze en Databricks.

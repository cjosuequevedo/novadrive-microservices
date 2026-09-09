# Lecciones transversales (Sistema 1 → Sistema 2)

> Escrito al cerrar la sesión de **Andes Motors** (Sistema 1 de AutoNova), para que la sesión que arranque **NovaDrive** (Sistema 2 — Postgres, microservicios, inglés) no tenga que redescubrir nada de esto por las malas. Todo lo de acá es agnóstico del sistema: nada de tablas `AM_*`, nada de español de UI, nada específico de Oracle salvo donde se aclara explícitamente como tal.
>
> Si estás arrancando NovaDrive desde cero: leé esto entero antes de tocar código. No asumas que un patrón que funcionó en Andes aplica igual acá solo porque "es el mismo pipeline" — varias de estas lecciones son justamente sobre verificar en vez de asumir.

## 1. Patrón outbox + worker

El patrón en sí (tabla de eventos técnica, sin FKs, con `PENDING/PROCESSING/RETRY/PUBLISHED/FAILED`, worker que reclama un lote con lock, publica, marca) es agnóstico de motor de base de datos — eso sí se traslada tal cual.

**Lo que NO se traslada sin verificar:** en Oracle, combinar `FOR UPDATE` con `FETCH FIRST n ROWS ONLY` (o cualquier forma de `LIMIT`) en la misma consulta revienta con `ORA-02014`. La solución que se usó en Andes fue partir el reclamo del lote en dos pasos:
1. Un `SELECT` **sin lock** que trae solo las claves primarias candidatas, con el `LIMIT` puesto ahí.
2. Un segundo `SELECT ... WHERE id IN (...) FOR UPDATE SKIP LOCKED` **sin** `LIMIT`, sobre esa lista puntual de IDs.

**Antes de portar este patrón de dos pasos a Postgres: verificalo, no lo asumas.** Postgres, a diferencia de Oracle, sí soporta combinar `SELECT ... FOR UPDATE SKIP LOCKED LIMIT n` en una sola consulta — es el patrón estándar y documentado para colas con Postgres (aparece así en la documentación oficial y en cualquier blog serio sobre "Postgres como cola"). Si es así, el equivalente en Postgres probablemente necesita **una sola consulta**, no dos — el patrón de dos pasos sería complejidad heredada de una limitación que no existe del otro lado. Dicho eso: no lo des por sentado solo por lo que dice este párrafo — corré un `EXPLAIN` real contra la versión de Postgres que se vaya a usar, con el driver/ORM real (SQLAlchemy + `psycopg`/`asyncpg`), antes de decidir la forma final del reclamo de lote. La lección de fondo no es "Postgres no tiene este problema", es **"no asumas que una limitación de un motor aplica a otro sin probarlo primero"** — así se perdió tiempo real en Andes al principio, antes de encontrar la combinación exacta que rompía.

## 2. Redpanda / Kafka

### Conectividad entre contenedores — este problema es de Kafka, no de Redpanda

El protocolo de Kafka (y todo lo que lo hable, Redpanda incluido) hace un **handshake de bootstrap y después redirige al cliente a la dirección que el broker anuncia como suya** (`advertise-kafka-addr` / `advertised.listeners`). Si el broker corre en Docker y anuncia `127.0.0.1`, eso funciona para procesos sueltos en el host, pero **no funciona desde otro contenedor** — `127.0.0.1` dentro de un contenedor apunta al propio contenedor, no al host. La conexión inicial puede parecer exitosa y después fallar en la redirección, lo cual es confuso de diagnosticar si no sabés que el protocolo hace ese segundo salto.

Solución aplicada: correr el broker con **dos listeners simultáneos**, cada uno con su propia dirección anunciada:
- Uno para procesos sueltos en el host (`127.0.0.1`).
- Uno para contenedores en la misma red de Docker (`host.docker.internal`), con su propio puerto publicado.

Esto es un problema general de cualquier broker Kafka-compatible corriendo en Docker con clientes tanto dentro como fuera de contenedores — no es una rareza de Redpanda. Si NovaDrive corre su propio broker (o el mismo Redpanda compartido) en un entorno mixto host/contenedores, replicá el listener dual desde el principio en vez de descubrirlo por el mismo camino de "conexión inicial ok, después falla".

### Por qué self-hosted y no Redpanda Cloud Serverless

Se evaluó y se descartó explícitamente: Redpanda Cloud Serverless es un **trial de 30 días con $100 de crédito**, no un tier gratuito permanente. El pedido del equipo fue algo gratis indefinidamente, así que se fue por Redpanda self-hosted en Docker — mismo patrón que hubiera aplicado con Kafka real, pero con un footprint de memoria mucho menor (ver sección 4). Si NovaDrive comparte el mismo Redpanda o levanta uno propio, la misma restricción de presupuesto aplica — no asumas que hay crédito de cloud disponible sin confirmarlo primero.

## 3. Databricks

Todo esto aplica igual si el alcance de NovaDrive todavía incluye streaming hacia Bronze — no es específico de Andes ni de Oracle.

### Serverless no soporta streaming "verdadero" en el código Spark

`Trigger.ProcessingTime()` y `Trigger.Continuous()` fallan en serverless con `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`. La solución **no** es buscar una excepción o un modo alternativo en el notebook — es no usarlos nunca ahí. Los notebooks de streaming deben usar siempre `trigger(availableNow=True)`, sin excepción.

El efecto "continuo" se logra en una capa completamente distinta: el campo `continuous.pause_status` (`UNPAUSED`/`PAUSED`) del **Job** vía la Jobs API — no en el código Spark. Con `pause_status=UNPAUSED`, el scheduler de Databricks dispara un run nuevo automáticamente en cuanto el anterior termina, indefinidamente, sin que nadie lo toque. Esto se **verificó hoy con evidencia real**, no es solo lo que dice la documentación: se prendió el Job y se observó la lista de Runs durante ~4.7 minutos reales, sin tocar nada más — aparecieron 8 runs consecutivos disparados solos, cada ciclo de 20 a 40 segundos, uno arrancando en el mismo segundo en que terminaba el anterior.

### La falsa percepción de "se apaga solo"

Justamente porque cada ciclo dura 20-40 segundos, si mirás el historial de Runs una sola vez después de ver "Succeeded" y no volvés a refrescar en esa ventana, da la sensación de que se detuvo cuando en realidad ya está en el ciclo siguiente. No es un bug ni una limitación real — es que el panel/tu observación no se actualiza tan rápido como el ciclo. Antes de reportar "el Job continuo no se sostiene solo" como un problema, observá con paciencia (varios minutos, refrescando) antes de concluir que se apagó.

### La lección de costos: no fue el Job, fue el Warehouse

Un gasto real e inesperado de crédito en un día se investigó con `system.billing.usage`/`system.billing.list_prices` y se encontró que el Job continuo cuesta centavos por hora — el 90% del gasto vino de un **SQL Warehouse reiniciándose una y otra vez** por consultas de verificación sueltas, espaciadas a lo largo del día (cada reinicio de un warehouse serverless factura de nuevo, aunque la consulta en sí sea trivial). La corrección no fue técnica, fue de hábito: agrupar las consultas de verificación en ráfagas cortas y **detener el warehouse explícitamente** al terminar cada ráfaga, en vez de dejarlo reiniciarse solo entre consultas dispersas. Si NovaDrive usa el mismo (o un warehouse propio), este hábito se traslada directo.

### Run Now (manual) vs. modo continuo — no son lo mismo, y ambos dejan rastro

Un `run-now` puntual (trigger `ONE_TIME`) corre el Job una vez y termina — no reactiva el modo continuo ni lo afecta. Es útil para procesar algo puntual sin prender el streaming completo, pero **no reemplaza al modo continuo** para una demo en vivo: en un `availableNow` de un solo disparo, la tarea de Silver puede tomar su snapshot de Bronze *antes* de que Bronze confirme el commit del dato más reciente (las dos tareas corren en paralelo, sin `depends_on`, porque un Job con trigger `continuous` no lo permite) — un evento recién llegado puede quedar afuera de esa pasada puntual y aparecer recién en la siguiente. En modo continuo real esto se autocorrige solo en el siguiente ciclo (segundos después); en un `run-now` único, no hay "siguiente ciclo" a menos que se dispare de nuevo a mano.

Los `run-now` manuales quedan registrados con `trigger: ONE_TIME` y `creator_user_name` real en el historial — sirven como evidencia auditable de quién disparó qué y cuándo, útil para diagnosticar "¿por qué apareció este dato si el Job estaba pausado?" sin especular.

### Los dos scripts, y por qué el de verificar es el que importa

Patrón de dos scripts (encender / apagar) más un tercero de verificación, en vez de confiar en el mensaje de éxito del primero:
- **Encender**: cambia `pause_status` a `UNPAUSED` vía `jobs/reset`.
- **Apagar**: cambia `pause_status` a `PAUSED` vía `jobs/reset`. El run que esté en curso en ese momento no se corta instantáneo — se deja terminar/cancelar solo.
- **Verificar**: consulta `pause_status` **y** la lista de runs activos (`runs/list?active_only=true`). Este es el que da certeza real — "apagar" devuelve éxito apenas se envía el cambio de estado, no espera a que el run en curso termine. Sin el paso de verificar (con una pequeña espera antes, ~10s), se puede reportar "apagado" mientras todavía hay cómputo corriendo y facturando.

**Nunca usar `run-now`/`cancel-all-runs` para prender o apagar el modo continuo** — esos endpoints son para jobs de trigger manual. El modo continuo se controla únicamente con `continuous.pause_status` vía `jobs/reset`.

## 4. Despliegue

### La lotería de capacidad de OCI (ARM, Always Free) vs. la simplicidad de un VPS x86 pago

El intento real de aprovisionar una VM `A1.Flex` (Ampere, ARM) del tier Always Free de Oracle Cloud falló con **"out of capacity"** — no fue un error de configuración, es una restricción de disponibilidad real y conocida de ese shape específico del tier gratuito (está notoriamente sobre-demandado). No vale la pena reintentar sin evaluar antes la alternativa de un VPS x86 pago (DigitalOcean, Hetzner) de costo bajo (~$5-6/mes) — la simplicidad y disponibilidad inmediata compensan el costo mínimo, sobre todo bajo presión de fecha.

Yendo a ARM además se hereda un riesgo real, no solo teórico: `confluent-kafka` (el cliente de Kafka en Python) **no publica wheels prebuilt para `linux/arm64`** en PyPI — solo hay `sdist`, lo que obliga a compilar la extensión C desde código fuente contra `librdkafka-dev`/`build-essential` dentro del Dockerfile, un paso que nunca se llegó a verificar realmente en una VM ARM real en esta sesión (quedó como riesgo sin cerrar). Yendo a x86_64 este problema directamente no existe — hay wheel prebuilt, `pip` lo prioriza automáticamente, y esas dependencias de compilación en el Dockerfile quedan sin uso real (inofensivas si se dejan, pero innecesarias).

### `ufw` antes de Docker, no después

Se activó el firewall (`ufw`, con reglas explícitas allow 22/80 + `default deny incoming`) **antes** de instalar Docker en la VM, no después. Docker manipula `iptables` directamente al publicar puertos de contenedores, y el orden/interacción entre las reglas de Docker y las de `ufw` puede ser confuso de razonar si Docker ya está corriendo con contenedores exponiendo puertos cuando recién ahí se activa el firewall. Configurar el firewall primero, con la política restrictiva ya en pie, evita esa ambigüedad — cuando Docker arranca después, ya hay una postura de "todo cerrado salvo lo explícitamente permitido" establecida de antemano.

### Memoria en una VM chica (4GB reales, no nominales)

Con Oracle + Redpanda + la app corriendo juntos en una VM de "4GB" que en la práctica reporta 3.8GB reales, el margen es angosto y hay que tratarlo como tal, no asumir que "4GB alcanza para todo":
- Medí el consumo real de cada pieza en local (`docker stats`) antes de estimar si algo nuevo entra en la VM de destino — no calcules a ciegas.
- Un motor de base de datos relacional completo (Oracle en este caso, pero aplica el mismo cuidado a Postgres) suele reservar memoria de forma bastante fija según su propia configuración por defecto (acá: `sga_target`+`pga_aggregate_target` ≈ 2GB, ya fijado en el `init.ora` de la imagen usada, no "crece con la carga" tanto como se podría pensar) — revisá la config real de memoria de la imagen/motor que uses antes de asumir cuánto va a pedir.
- Ponele `mem_limit` explícito a cada servicio del compose, aunque el motor ya tenga su propia config interna — es una capa de seguridad adicional, no redundante.
- Agregá **swap** como red de seguridad barata si hay disco de sobra (acá, 2GB de swap con 114GB de disco libre) — convierte un posible OOM-kill duro en degradación más lenta en vez de un proceso muerto sin aviso en medio de una demo.
- Verificá el margen bajo actividad real, no solo en idle — en esta sesión, tras ~20 minutos de actividad real (no carga pesada), el sistema ya había empezado a usar el swap (200MB) — confirma que el margen calculado de antemano era ajustado de verdad, no una estimación conservadora de sobra.

## 5. Seguridad operativa

**Nunca le pidas a un humano que escriba o pegue un secreto real (token, contraseña, clave) en un comando o en el chat.** En esta sesión ya pasó una vez en un proyecto anterior relacionado: un token terminó pegado en texto plano en una guía, tuvo que revocarse y regenerarse. La solución de fondo, aplicada acá y a replicar desde el día uno en NovaDrive:
- Cualquier script que necesite una credencial la lee sola de un archivo `.env` (con un helper tipo `cargar_dotenv_si_falta()` que respeta una variable ya exportada y solo rellena desde `.env` si falta) — nunca exigir un `export VAR=...` manual en la terminal como paso de una guía, porque ese paso manual es justamente lo que lleva a que alguien copie/pegue el valor real en algún lado que no debería.
- Cuando hace falta un secreto **nuevo** (contraseña de una base, credencial de una app), generalo con una librería criptográfica (`secrets` en Python, no `random`), nunca le pidas al humano que lo invente o lo tipee.
- Si el humano necesita guardar ese secreto nuevo en su propio gestor de contraseñas, mostráselo **una sola vez**, de forma clara y copiable, y después no lo repitas más en la conversación — ni en resúmenes, ni en confirmaciones posteriores.
- Cualquier archivo con secretos reales (`.env`, `RUNBOOK.md` con credenciales) queda gitignored desde el commit inicial del proyecto, con una plantilla `.example` sin valores reales como la única versión commiteada.

## 6. Metodología de trabajo que valió la pena

Para que la sesión nueva la aplique desde el minuto uno, no la descubra después de gastar horas:

- **Diagnosticar antes de implementar, siempre que haya margen de duda.** Antes de escribir configuración alrededor de un componente de terceros (una imagen de Docker, una API externa), revisá su comportamiento real — el script de arranque, la documentación de la versión exacta que estás usando, un endpoint de solo lectura — en vez de asumir que se comporta como recordás de otro contexto. Ejemplo concreto de hoy: antes de depender de que una imagen de base de datos soportara crear un usuario de aplicación automáticamente vía variables de entorno, se verificó leyendo el script de entrypoint real dentro del propio contenedor, no se asumió por el nombre de las variables.
- **Pedir evidencia real en cada checkpoint, nunca aceptar "ya debería funcionar".** Resultado real de una query, log real con timestamps, código de estado HTTP real, captura de pantalla real. Si algo no se pudo verificar (falta de credenciales, entorno no disponible), decirlo explícitamente en vez de asumir éxito por inferencia — y esto aplica también a las propias afirmaciones de quien está armando el sistema, no solo a lo que pide el humano.
- **Marcar explícitamente las decisiones de diseño que el documento fuente no especifica, en vez de asumirlas en silencio.** Cuando una especificación deja un hueco (cómo representar un número decimal en JSON, qué arquitectura de red usar entre contenedores, cuánta memoria asignarle a cada servicio), tomar una decisión razonable está bien — pero hay que **dejarla anotada como decisión, con su razón**, para que quien la lea después sepa que fue elegida a propósito y no es un accidente ni algo que el documento exigía.
- **Cuando algo no se comporta como se esperaba, investigar el mecanismo real antes de reportar un bug (o antes de descartarlo como "no es nada").** El hallazgo de la condición de carrera entre las tareas de Bronze y Silver en esta sesión no salió de adivinar — salió de reproducir el caso dos veces (una que fallaba, otra idéntica que no) y comparar la diferencia real entre ambas corridas.

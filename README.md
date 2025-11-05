````markdown
# Servicio de RENIEC

Servicio **RENIEC** para verificación de identidad.  
Escucha solicitudes por **RabbitMQ (RPC)**, consulta **BD2 (RENIEC)** y responde con los datos de la persona.

---

## Base de Datos — RENIEC (BD2)

Para crear e inicializar la base de datos, **basta con ejecutar `App.java`**.  
El servidor usa **SQLite** (`data/reniec.db`) y aplica automáticamente `schema.sql` + `seed.sql` si la tabla no existe.

### 🧾 TABLA: PERSONAS
Contiene el padrón mínimo para validación de identidad.

| Columna              | Tipo | Restricciones | Descripción |
|----------------------|------|---------------|-------------|
| **dni**              | TEXT | PRIMARY KEY | DNI en 8 dígitos. |
| **apell_pat**        | TEXT | NOT NULL | Apellido paterno. |
| **apell_mat**        | TEXT | NOT NULL | Apellido materno. |
| **nombres**          | TEXT | NOT NULL | Nombres. |
| **fecha_naci**       | TEXT | NOT NULL | Fecha de nacimiento (`YYYY-MM-DD`). |
| **sexo**             | TEXT | CHECK (sexo IN ('M','F')) | Sexo biológico. |
| **estado_civil**     | TEXT | NOT NULL | Estado civil. |
| **lugar_nacimiento** | TEXT | NOT NULL | Lugar de nacimiento. |
| **direccion**        | TEXT | — | Dirección. |

### ⚙️ Notas
- La BD se inicializa en el **primer arranque** (auto-init) desde los recursos del proyecto.
- Los campos `estado_civil` y `lugar_nacimiento` están incluidos para alinear con el documento del curso.
- SQLite no requiere `CREATE USER` / `CREATE DATABASE`; es un archivo local.

---

## 📬 Contrato de Mensajería — RENIEC (RabbitMQ)

Define **cómo RENIEC recibe consultas** del Banco y **cómo responde**. El patrón es **RPC** (el solicitante envía `reply_to` y `correlation_id`).

### 0) Convenciones comunes

**Exchange (direct):** `verify_exchange`  
**Routing key:** `verify`  
**Cola de entrada RENIEC:** `verify_queue` (enlazada a `verify_exchange` con `verify`)  
**Formato de mensajes:** JSON UTF-8

> **Compatibilidad:** Si el banco ya publica a `reniec.verify`, se puede enlazar esa cola adicionalmente (opcional).  
> El servidor declara/bindea `verify_queue` al iniciar; si ya existe, la reutiliza.

#### 0.1 Encabezados AMQP (obligatorios en toda petición a RENIEC)
- `reply_to`: cola del solicitante para recibir la respuesta.
- `correlation_id`: UUID de correlación (reutilizado tal cual en la respuesta).
- (Opcional) `expiration`: TTL del mensaje.

---

### 1) Operación — `VerifyIdentity` (Banco → RENIEC)

**Body (request)**
```json
{ "dni": "45678912" }
````

**Body (response — OK, encontrado)**

```json
{
  "ok": true,
  "person": {
    "dni": "45678912",
    "apell_pat": "GARCÍA",
    "apell_mat": "FLORES",
    "nombres": "MARÍA ELENA",
    "fecha_naci": "1990-07-15",
    "sexo": "F",
    "estado_civil": "SOLTERO",
    "lugar_nacimiento": "Lima",
    "direccion": "Av. Universitaria 1234"
  },
  "error": null
}
```

**Body (response — NO encontrado)**

```json
{ "ok": false, "person": null, "error": "NOT_FOUND" }
```

> **Reglas:**
>
> * `dni` debe ser cadena de **8 dígitos**.
> * Si el `dni` no existe en `PERSONAS`, la respuesta es `ok=false` con `error="NOT_FOUND"`.

---

## 🔁 Flujo RPC (resumen)

1. **Banco → Exchange**: publica en `verify_exchange` con `routing_key=verify`.
   Encabezados: `reply_to=<cola del banco>`, `correlation_id=<uuid>`; Body: `{ "dni": "..." }`.

2. **RENIEC** consume de `verify_queue`, consulta BD2 y **responde** a la cola indicada en `reply_to`, copiando `correlation_id`.

3. **Banco** lee su cola `reply_to` y empareja por `correlation_id`.

---

## 🧪 Pruebas

### UI de RabbitMQ (sin escribir código)

1. Crea una cola temporal (p. ej., `test.reply`).
2. En **Exchanges → `verify_exchange` → Publish message**:

   * Routing key: `verify`
   * Properties → `reply_to`: `test.reply`
   * Properties → `correlation_id`: `t1`
   * Payload:

     ```json
     {"dni":"12345678"}
     ```
3. Ve a **Queues → `test.reply` → Get messages** y verifica la respuesta.

---

## 🧠 Reglas y validaciones (resumen)

* `dni`: exactamente 8 dígitos (`^[0-9]{8}$`).
* La respuesta siempre es **determinística** para un `dni` dado (lectura de BD).
* No se requiere idempotencia a nivel de servidor (operación de **solo lectura**).

---

## 📦 Dependencias clave (Maven)

* `com.rabbitmq:amqp-client` — Cliente RabbitMQ.
* `com.fasterxml.jackson.core:jackson-databind` — JSON.
* `org.xerial:sqlite-jdbc` — Driver SQLite.
* `com.zaxxer:HikariCP` — Pool JDBC.
* `org.slf4j:slf4j-simple` — Logging a consola.

---

## 🛠️ Troubleshooting

* **Sin logs / SLF4J NOP** → agregar `slf4j-simple` en dependencias y usar classpath `runtime` al ejecutar.
* **`data/reniec.db` no se crea** → crear carpeta `data/` o usar ruta absoluta; el servidor intenta crearla automáticamente.
* **No hay consumidores en `verify_queue`** → asegurar que `App.java` esté corriendo y que `RABBIT_HOST/PORT/USER/PASS` sean correctos.
* **Sin respuesta RPC** → publicar SIEMPRE con `reply_to` + `correlation_id`; verificar que el `dni` exista en `seed.sql`.

---

## 🔗 Interoperabilidad con el Banco

* Banco publica a `verify_exchange`/`verify` y espera respuesta **RPC**.
* RENIEC responde con `{ ok, person, error }`.
  Si el banco requiere otro envoltorio (p. ej. `{ ok, data:{valid:...}, ... }`), puede adaptarse en el consumidor del banco o agregarse un formateo opcional en RENIEC (no cambia el flujo RPC).

---

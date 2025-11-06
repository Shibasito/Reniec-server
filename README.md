````markdown
# RENIEC Server (LP2 · Python) — RabbitMQ + PostgreSQL

Servicio RENIEC que valida DNIs para el **bank-server** vía **RabbitMQ**.  
Escucha en `reniec_queue` (exchange `rabbit_exchange`, routing key `reniec_operation`) y responde por **RPC** usando `reply_to` + `correlation_id`.  
La BD **PostgreSQL** se **auto-inicializa** al arrancar (tabla `personas` + seed opcional).

---

## 📦 Arquitectura (resumen)

- **Mensajería**: RabbitMQ (AMQP 0-9-1)
  - Exchange: `rabbit_exchange` (direct)
  - Queue: `reniec_queue`
  - Routing key: `reniec_operation`
  - RPC: el servicio responde al **default exchange** `""` con `routing_key = reply_to` y conserva `correlation_id`.

- **Base de datos**: PostgreSQL
  - Esquema por defecto: `public` (configurable con `PGSCHEMA`)
  - Tabla: `personas (dni, apell_pat, apell_mat, nombres, …)`
  - **Auto-init**: crea schema/tabla si no existen y puede sembrar data (seed).

- **Contrato de respuesta al bank**:
  ```json
  {
      "ok": true,
      "data": {
          "valid": true,
          "dni": "12345678",
          "nombres": "LUIS ALBERTO",
          "apellidoPat": "TORRES",
          "apellidoMat": "MENDOZA"
      }
  }
````

Si el DNI no existe: `"valid": false` (y el resto vacío).

---

## 📁 Estructura relevante del repo

* `reniec_server.py` → servicio principal (RabbitMQ + Postgres + auto-init).
* `requirements.txt` → dependencias de Python.
* `Dockerfile` → imagen del servicio (Python slim).
* `docker-compose.yml` → orquestación **(modo de ejecución ÚNICO)**.

> **Nota**: Este servicio **no** depende de ningún archivo/clase del antiguo servidor Java. Puedes borrar el código Java del reniec.


---

## ▶️ Cómo ejecutar

1. **Construir y levantar**:

   ```bash
   docker compose up -d --build
   ```

2. **Ver logs**:

   ```bash
   docker compose logs -f reniec-server
   ```

   Debes ver algo como:

   ```
   [DB] Connecting ... DSN=postgresql://reniec:***@postgres:5432/reniec
   [DB] schema/seed OK (schema=public, seed=True)
   [AMQP] Ready ex=rabbit_exchange rk=reniec_operation q=reniec_queue
   [*] RENIEC Python server listening...
   ```

3. **Inspeccionar la tabla** (si usas el Postgres del compose):

   ```bash
   docker compose exec postgres psql -U reniec -d reniec -c "\dt"
   docker compose exec postgres psql -U reniec -d reniec -c "SELECT * FROM personas LIMIT 5;"
   ```

---

## 🧪 Pruebas rápidas (desde RabbitMQ)

> Usa la UI de RabbitMQ (Management, puerto **15672**) del broker donde apuntas con `RABBIT_HOST`.
> **No** es necesario declarar nada extra: el servicio declara exchange/queue/bind de forma idempotente.

1. **Publicar la solicitud**:

   * Exchange: `rabbit_exchange`
   * Routing key: `reniec_operation`
   * **Properties**:

     * `reply_to`: `test_queue`  ← (cola temporal para leer la respuesta)
     * `correlation_id`: `test1`
   * Payload:

     ```json
     { "dni": "12345678" }
     ```

2. **Leer la respuesta** en `test_queue`:

   * Esperado (si el DNI existe):

     ```json
     { "ok": true, "data": { "valid": true, "dni": "12345678", "nombres": "...", "apellidoPat": "...", "apellidoMat": "..." } }
     ```
   * Si no existe:

     ```json
     { "ok": true, "data": { "valid": false, "dni": "12345678" } }
     ```

> El bank-server realiza la misma publicación, pero con su propia cola de respuesta y maneja el `correlation_id` internamente.

---

## 🔗 Integración con bank-server

* El bank publica a `rabbit_exchange` con `routing_key=reniec_operation`,
  usando:

  * `reply_to = <cola del bank>` (p.ej., `bank_queue` o la que use su RPC),
  * `correlation_id = <id del bank>`.
* `reniec-server` responde al **default exchange** `""` con `routing_key = reply_to` y **el mismo** `correlation_id`.
* Formato de respuesta: ver **Contrato** arriba.

---

## 🧰 Comandos útiles

```bash
# Reiniciar sólo el servicio RENIEC
docker compose restart reniec-server

# Reconstruir e iniciar
docker compose up -d --build

# Ver logs
docker compose logs -f reniec-server

# Shell en Postgres (si está en el compose)
docker compose exec -it postgres psql -U reniec -d reniec
```

---

#!/usr/bin/env python3
# reniec_server.py — Servicio RENIEC (RabbitMQ + PostgreSQL)
#
# Consume:
#   exchange: rabbit_exchange (direct)
#   queue   : reniec_queue
#   routing : reniec_operation
#
# Responde:
#   al default exchange "" usando properties.reply_to (mismo correlation_id)
#
# Respuesta compatible con bank-server:
#   { "ok": true, "data": { "valid": true|false, "dni": "...",
#                           "nombres": "...", "apellidoPat": "...", "apellidoMat": "..." } }
#
# Requisitos:
#   pip install pika psycopg[binary]
#
# AMQP ENV:
#   RABBIT_HOST=localhost
#   RABBIT_PORT=5672
#   RABBIT_USERNAME=admin
#   RABBIT_PASSWORD=admin
#   RABBIT_VHOST=/
#   RABBIT_EXCHANGE=rabbit_exchange
#   RABBIT_RENIEC_QUEUE=reniec_queue
#   RABBIT_RENIEC_ROUTING=reniec_operation
#
# DB ENV:
#   PG_DSN=postgresql://reniec:reniec@127.0.0.1:5432/reniec?sslmode=disable
#   (o componentes:)
#   PGHOST=127.0.0.1  PGPORT=5432  PGUSER=reniec  PGPASSWORD=reniec  PGDATABASE=reniec
#
# Seed/Schema ENV:
#   RENIEC_INIT_SCHEMA=1         # crea tabla si no existe
#   RENIEC_SEED_ENABLE=1         # activa el poblado de datos
#   RENIEC_SEED_BASE=10000000    # primer DNI del seed masivo
#   RENIEC_SEED_COUNT=10000      # cuántos DNIs generar (contiguos)
#
# Nota clave en este update:
#   - Forzamos el DNI recibido a string y lo normalizamos para evitar fallos
#     cuando el productor lo envía numérico (sin comillas).

import json
import os
import signal
import sys
from typing import Optional, Dict

import pika
import psycopg

# -------------------------
# Config AMQP
# -------------------------
RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USERNAME", "admin")
RABBIT_PASS = os.getenv("RABBIT_PASSWORD", "admin")
RABBIT_VHOST = os.getenv("RABBIT_VHOST", "/")

EXCHANGE = os.getenv("RABBIT_EXCHANGE", "rabbit_exchange")
RENIEC_QUEUE = os.getenv("RABBIT_RENIEC_QUEUE", "reniec_queue")
RENIEC_ROUTING = os.getenv("RABBIT_RENIEC_ROUTING", "reniec_operation")
PREFETCH = int(os.getenv("RABBIT_PREFETCH", "8"))

# -------------------------
# Config DB
# -------------------------
PG_DSN = os.getenv("PG_DSN")
if not PG_DSN:
    PGHOST = os.getenv("PGHOST", "127.0.0.1")
    PGPORT = int(os.getenv("PGPORT", "5432"))
    PGUSER = os.getenv("PGUSER", "reniec")
    PGPASSWORD = os.getenv("PGPASSWORD", "reniec")
    PGDATABASE = os.getenv("PGDATABASE", "reniec")
    PG_DSN = f"postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}?sslmode=disable"

# -------------------------
# Config Schema/Seed
# -------------------------
INIT_SCHEMA = os.getenv("RENIEC_INIT_SCHEMA", "1") == "1"
SEED_ENABLE = os.getenv("RENIEC_SEED_ENABLE", "1") == "1"
SEED_BASE = int(os.getenv("RENIEC_SEED_BASE", "10000000"))     # primer DNI del seed masivo
SEED_COUNT = int(os.getenv("RENIEC_SEED_COUNT", "10000"))      # cuántos DNIs insertar

# -------------------------
# DB helpers
# -------------------------
def open_db():
    # psycopg3 — conexión bloqueante
    conn = psycopg.connect(PG_DSN, autocommit=False)
    return conn

def ensure_schema(conn):
    if not INIT_SCHEMA:
        return
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS personas (
              dni              VARCHAR(8) PRIMARY KEY,
              apell_pat        TEXT NOT NULL,
              apell_mat        TEXT NOT NULL,
              nombres          TEXT NOT NULL,
              fecha_naci       DATE,
              sexo             CHAR(1),
              direccion        TEXT,
              estado_civil     TEXT,
              lugar_nacimiento TEXT
            );
        """)
    conn.commit()

def seed_db(conn):
    if not SEED_ENABLE:
        return
    with conn.cursor() as cur:
        # 1) Semilla fija con UPSERT (asegura que siempre existan con estos datos)
        cur.execute("""
            INSERT INTO personas
              (dni, apell_pat, apell_mat, nombres, fecha_naci, sexo,
               direccion, estado_civil, lugar_nacimiento)
            VALUES
              ('12345678','TORRES','MENDOZA','LUIS ALBERTO','1992-11-05','M','Sacsayhuamán 789','Casado','Lima'),
              ('23456789','PEREZ','GOMEZ','ANA MARIA','1990-03-12','F','Jr. Cusco 123','Soltera','Cusco'),
              ('34567890','RAMIREZ','LOPEZ','CARLOS ANDRÉS','1988-07-15','M','Av. Arequipa 456','Soltero','Lima'),
              ('45678901','FLORES','HUAMAN','MARÍA ELENA','1995-02-28','F','Jr. Amazonas 987','Soltera','Arequipa'),
              ('56789012','GARCIA','SALAZAR','JORGE LUIS','1998-09-10','M','Av. Los Héroes 320','Soltero','Arequipa'),
              ('67890123','ROJAS','QUISPE','LUCÍA FERNANDA','2000-05-21','F','Calle Lima 221','Soltera','Puno'),
              ('78901234','DIAZ','RAMOS','PEDRO MIGUEL','1985-12-03','M','Jr. Ayacucho 745','Casado','Trujillo')
            ON CONFLICT (dni) DO UPDATE
            SET apell_pat        = EXCLUDED.apell_pat,
                apell_mat        = EXCLUDED.apell_mat,
                nombres          = EXCLUDED.nombres,
                fecha_naci       = EXCLUDED.fecha_naci,
                sexo             = EXCLUDED.sexo,
                direccion        = EXCLUDED.direccion,
                estado_civil     = EXCLUDED.estado_civil,
                lugar_nacimiento = EXCLUDED.lugar_nacimiento;
        """)

        # 2) Semilla masiva parametrizable (no pisa los anteriores)
        cur.execute(f"""
            WITH s AS (
                SELECT generate_series(0, {SEED_COUNT}-1) AS i
            )
            INSERT INTO personas (dni, apell_pat, apell_mat, nombres, fecha_naci, sexo, direccion)
            SELECT
                to_char({SEED_BASE} + i, 'FM00000000')       AS dni,
                'APELLIDO' || i                               AS apell_pat,
                'MATERNO' || i                                AS apell_mat,
                'NOMBRE ' || i                                AS nombres,
                DATE '1990-01-01' + (i % 10000)               AS fecha_naci,
                CASE WHEN (i % 2) = 0 THEN 'M' ELSE 'F' END   AS sexo,
                'CALLE ' || i                                 AS direccion
            FROM s
            ON CONFLICT (dni) DO NOTHING;
        """)
    conn.commit()


def init_db(conn):
    ensure_schema(conn)
    seed_db(conn)
    # Diagnóstico: contar filas y validar primer DNI del seed
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM personas;")
        total = cur.fetchone()[0]
        probe = f"{SEED_BASE:08d}"
        cur.execute("SELECT 1 FROM personas WHERE dni = %s;", (probe,))
        has_probe = cur.fetchone() is not None
    print(f"[DB] personas={total}  probe({probe})={has_probe}", flush=True)

def normalize_dni(value) -> Optional[str]:
    """
    Convierte lo que venga (int/str/None) a un DNI string 8 dígitos si es válido.
    """
    if value is None:
        return None
    try:
        s = str(value).strip()
    except Exception:
        return None
    # eliminar espacios, comillas raras, etc.
    s = "".join(ch for ch in s if ch.isdigit())
    if len(s) == 8 and s.isdigit():
        return s
    return None

def get_person(conn, dni_input) -> Optional[Dict]:
    """
    Devuelve dict con campos esperados por el bank o None si no existe/ inválido.
    """
    dni = normalize_dni(dni_input)
    if not dni:
        return None
    sql = """
        SELECT dni, apell_pat, apell_mat, nombres, fecha_naci, sexo, direccion, estado_civil, lugar_nacimiento
        FROM personas WHERE dni = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (dni,))
        row = cur.fetchone()
        conn.rollback()
        if not row:
            return None
        (dni, ap_pat, ap_mat, nombres, fecha_naci, sexo, direccion, estado_civil, lugar_nacimiento) = row
        out = {
            "dni": dni,
            "nombres": nombres,
            "apellidoPat": ap_pat,
            "apellidoMat": ap_mat,
        }
        # opcionales para debug/cliente
        if fecha_naci is not None:
            out["fecha_naci"] = str(fecha_naci)
        if sexo is not None:
            out["sexo"] = sexo
        if direccion is not None:
            out["direccion"] = direccion
        if estado_civil is not None:
            out["estado_civil"] = estado_civil
        if lugar_nacimiento is not None:
            out["lugar_nacimiento"] = lugar_nacimiento
        return out

# -------------------------
# RabbitMQ server
# -------------------------
class ReniecRpcServer:
    def __init__(self):
        self._closing = False
        self._conn = None
        self._ch = None
        self._db = None

    def connect_db(self):
        self._db = open_db()
        init_db(self._db)
        print(f"[DB] Connected to {PG_DSN}", flush=True)

    def connect_rabbit(self):
        creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
        params = pika.ConnectionParameters(
            host=RABBIT_HOST,
            port=RABBIT_PORT,
            virtual_host=RABBIT_VHOST,
            credentials=creds,
            heartbeat=30,
            blocked_connection_timeout=60,
            client_properties={"connection_name": "reniec-server-py"},
        )
        self._conn = pika.BlockingConnection(params)
        self._ch = self._conn.channel()
        self._ch.basic_qos(prefetch_count=PREFETCH)

        # Declaraciones idempotentes por si el middleware aún no levantó todo
        self._ch.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
        self._ch.queue_declare(queue=RENIEC_QUEUE, durable=True)
        self._ch.queue_bind(queue=RENIEC_QUEUE, exchange=EXCHANGE, routing_key=RENIEC_ROUTING)

        print(f"[AMQP] Ready  ex={EXCHANGE} rk={RENIEC_ROUTING} q={RENIEC_QUEUE}", flush=True)

    def start(self):
        self.connect_db()
        self.connect_rabbit()

        def on_message(ch, method, props, body: bytes):
            corr_id = getattr(props, "correlation_id", None)
            reply_to = getattr(props, "reply_to", None)

            try:
                preview = body.decode("utf-8", errors="replace")
            except Exception:
                preview = "<binary>"
            print(f"[>] Received corr={corr_id} reply_to={reply_to} size={len(body)} body={preview}", flush=True)

            # Parseo robusto: acepta {"dni": ...} o {"data":{"dni":...}} o {"payload":{"dni":...| "usuario":...}}
            dni = None
            try:
                req = json.loads(preview)
                raw_dni = (
                    req.get("dni")
                    or ((req.get("data") or {}) or {}).get("dni")
                    or ((req.get("payload") or {}) or {}).get("dni")
                    or ((req.get("payload") or {}) or {}).get("usuario")
                )
                dni = normalize_dni(raw_dni)
            except Exception:
                dni = None

            try:
                person = get_person(self._db, dni) if dni else None
                if person:
                    data = {"valid": True, **person}
                else:
                    data = {"valid": False, "dni": dni or ""}
                resp = {"ok": True, "data": data}
            except Exception as e:
                resp = {"ok": False, "data": None, "error": {"message": str(e)}}

            payload = json.dumps(resp, ensure_ascii=False).encode("utf-8")
            props_out = pika.BasicProperties(
                correlation_id=corr_id,
                content_type="application/json",
            )

            try:
                if not reply_to:
                    print(f"[!] Missing reply_to; dropping response corr={corr_id}", flush=True)
                else:
                    self._ch.basic_publish(
                        exchange="",
                        routing_key=reply_to,
                        properties=props_out,
                        body=payload,
                    )
                    print(f"[<] Sent corr={corr_id} to={reply_to} size={len(payload)} body={payload.decode('utf-8')}", flush=True)
            finally:
                ch.basic_ack(delivery_tag=method.delivery_tag)

            # Ping DB; si muere, reabrimos
            try:
                self._db.execute("SELECT 1;")
                self._db.rollback()
            except Exception:
                try:
                    self._db.close()
                except Exception:
                    pass
                self.connect_db()

        self._ch.basic_consume(queue=RENIEC_QUEUE, on_message_callback=on_message, auto_ack=False)
        print("[*] RENIEC Python server listening... Ctrl+C para salir", flush=True)

        try:
            self._ch.start_consuming()
        except KeyboardInterrupt:
            self.stop()
        except pika.exceptions.AMQPError as e:
            print(f"[AMQP ERROR] {e}", flush=True)
            self.stop()

    def stop(self):
        if self._closing:
            return
        self._closing = True
        try:
            if self._ch and self._ch.is_open:
                self._ch.close()
        except Exception:
            pass
        try:
            if self._conn and self._conn.is_open:
                self._conn.close()
        except Exception:
            pass
        try:
            if self._db:
                self._db.close()
        except Exception:
            pass
        print("[*] Stopped cleanly", flush=True)


def main():
    srv = ReniecRpcServer()

    def _sig_handler(signum, frame):
        srv.stop()
        sys.exit(0)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _sig_handler)
        except Exception:
            pass

    srv.start()


if __name__ == "__main__":
    main()

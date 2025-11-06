#!/usr/bin/env python3
# reniec_server.py
# Servicio RENIEC en Python (LP2) – RabbitMQ + PostgreSQL
#
# - Consume de:   exchange = "rabbit_exchange" (direct)
#                 queue    = "reniec_queue"
#                 routing  = "reniec_operation"
# - Responde a:   default exchange "" hacia properties.reply_to
#                 (manteniendo correlation_id)
# - Formato de salida compatible con bank-server:
#     { "ok": true, "data": { "valid": true|false, "dni": "...",
#                              "nombres": "...", "apellidoPat": "...", "apellidoMat": "..." } }
#
# Requisitos (pip):
#   pip install pika psycopg[binary]
#
# Variables de entorno útiles:
#   RABBIT_HOST=localhost
#   RABBIT_PORT=5672
#   RABBIT_USERNAME=admin
#   RABBIT_PASSWORD=admin
#   RABBIT_VHOST=/            (opcional)
#   RABBIT_EXCHANGE=rabbit_exchange
#   RABBIT_RENIEC_QUEUE=reniec_queue
#   RABBIT_RENIEC_ROUTING=reniec_operation
#
#   PGHOST=127.0.0.1
#   PGPORT=5432
#   PGUSER=reniec
#   PGPASSWORD=reniec
#   PGDATABASE=reniec
#   # o bien:
#   PG_DSN=postgresql://reniec:reniec@127.0.0.1:5432/reniec?sslmode=disable
#
# Esquema esperado (PostgreSQL):
#   CREATE TABLE personas (
#     dni VARCHAR(8) PRIMARY KEY,
#     apell_pat TEXT NOT NULL,
#     apell_mat TEXT NOT NULL,
#     nombres   TEXT NOT NULL,
#     fecha_naci DATE,
#     sexo CHAR(1),
#     direccion TEXT,
#     estado_civil TEXT,
#     lugar_nacimiento TEXT
#   );

import json
import os
import signal
import sys
import time
from typing import Optional, Dict

import pika
import psycopg


# -------------------------
# Config
# -------------------------
RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USERNAME", "admin")
RABBIT_PASS = os.getenv("RABBIT_PASSWORD", "admin")
RABBIT_VHOST = os.getenv("RABBIT_VHOST", "/")

EXCHANGE = os.getenv("RABBIT_EXCHANGE", "rabbit_exchange")
RENIEC_QUEUE = os.getenv("RABBIT_RENIEC_QUEUE", "reniec_queue")
RENIEC_ROUTING = os.getenv("RABBIT_RENIEC_ROUTING", "reniec_operation")

# DB por DSN (si existe) o por componentes PG*
PG_DSN = os.getenv("PG_DSN")
if not PG_DSN:
    PGHOST = os.getenv("PGHOST", "127.0.0.1")
    PGPORT = int(os.getenv("PGPORT", "5432"))
    PGUSER = os.getenv("PGUSER", "reniec")
    PGPASSWORD = os.getenv("PGPASSWORD", "reniec")
    PGDATABASE = os.getenv("PGDATABASE", "reniec")
    PG_DSN = f"postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}?sslmode=disable"

# Prefetch básico para RPC
PREFETCH = int(os.getenv("RABBIT_PREFETCH", "8"))

# -------------------------
# DB helpers
# -------------------------
def open_db():
    # psycopg3 – connection única (Blocking), autocommit OFF; haremos solo SELECT.
    conn = psycopg.connect(PG_DSN, autocommit=False)
    return conn

def init_db(conn):
    """Crea tabla e inserta datos iniciales si no existen"""
    with conn.cursor() as cur:
        # Crear tabla
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
        
        # Insertar datos de prueba
        cur.execute("""
            INSERT INTO personas (dni, apell_pat, apell_mat, nombres, fecha_naci, sexo, direccion)
            VALUES
            ('12345678','TORRES','MENDOZA','LUIS ALBERTO','1992-11-05','M','Sacsayhuamán 789'),
            ('23456789','PEREZ','GOMEZ','ANA MARIA','1990-03-12','F','Jr. Cusco 123'),
            ('34567890','QUISPE','HUAMAN','JOSE CARLOS','1988-01-20','M','Av. Grau 456'),
            ('45678901','RAMIREZ','LOPEZ','MARIA ELENA','1995-07-08','F','Av. Arequipa 1020'),
            ('56789012','ROJAS','SALAZAR','CARLOS ANDRÉS','1993-05-17','M','Jr. Junín 321'),
            ('67890123','FLORES','CASTILLO','KAREN LUCÍA','1998-11-30','F','Psje. Libertad 55'),
            ('78901234','DIAZ','PALOMINO','SERGIO ARTURO','1987-02-14','M','Calle Los Olivos 12'),
            ('89012345','GARCIA','MORI','NATALIA SOFÍA','1999-09-22','F','Av. La Marina 200'),
            ('90123457','CHÁVEZ','RIVAS','FRANCISCO JAVIER','1991-04-03','M','Jr. Tarapacá 88'),
            ('01234568','MENDOZA','CRUZ','VALERIA PAOLA','2000-12-01','F','Malecón Balta 300'),
            ('11223344','SANCHEZ','ARIAS','EDUARDO MANUEL','1994-06-15','M','Calle Colmena 17'),
            ('55667788','AGUILAR','VERA','ANDREA NICOLE','1996-10-05','F','Jr. Puno 740')
            ON CONFLICT (dni) DO NOTHING;
        """)
        
        conn.commit()
        print("[DB] Tabla 'personas' inicializada", flush=True)

def get_person(conn, dni: str) -> Optional[Dict]:
    """
    Devuelve un dict con los campos de la persona o None si no existe.
    Mapea columnas a los nombres que el bank consume:
      - apell_pat  -> apellidoPat
      - apell_mat  -> apellidoMat
    """
    if not dni or len(dni) != 8 or not dni.isdigit():
        return None
    sql = "SELECT dni, apell_pat, apell_mat, nombres, fecha_naci, sexo, direccion, estado_civil, lugar_nacimiento FROM personas WHERE dni = %s"
    with conn.cursor() as cur:
        cur.execute(sql, (dni,))
        row = cur.fetchone()
        conn.rollback()  # no dejamos transacción abierta
        if not row:
            return None
        (dni, ap_pat, ap_mat, nombres, fecha_naci, sexo, direccion, estado_civil, lugar_nacimiento) = row
        out = {
            "dni": dni,
            "nombres": nombres,
            "apellidoPat": ap_pat,
            "apellidoMat": ap_mat,
        }
        # Campos opcionales, por si te sirven para debug/cliente
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

        # Idempotente: por si el middleware aún no corrió
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

            # Log entrada
            try:
                preview = body.decode("utf-8", errors="replace")
            except Exception:
                preview = "<binary>"
            print(f"[>] Received corr={corr_id} reply_to={reply_to} size={len(body)} body={preview}", flush=True)

            # Parse request (acepta {"dni":"..."} o {"data":{"dni":"..."}})
            dni = None
            try:
                req = json.loads(preview)
                dni = req.get("dni") or (req.get("data", {}) or {}).get("dni")
            except Exception as e:
                pass

            try:
                person = get_person(self._db, dni) if dni else None
                if person:
                    data = {"valid": True, **person}
                else:
                    data = {"valid": False, "dni": (dni or "")}

                resp = {"ok": True, "data": data}
            except Exception as e:
                # Falla de DB u otra: ok=false
                resp = {"ok": False, "data": None, "error": {"message": str(e)}}

            payload = json.dumps(resp, ensure_ascii=False).encode("utf-8")

            # Publica respuesta RPC al default exchange "" → reply_to
            props_out = pika.BasicProperties(
                correlation_id=corr_id,
                content_type="application/json",
            )

            try:
                if not reply_to:
                    # No hay dónde responder; solo ack y log
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

            # Mantener DB saludable: si el cursor/conn muere, reabrimos en el próximo msg
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
    # Señales para cierre limpio (Docker/Unix)
    server = ReniecRpcServer()

    def _sig_handler(signum, frame):
        server.stop()
        sys.exit(0)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _sig_handler)
        except Exception:
            pass

    server.start()


if __name__ == "__main__":
    main()

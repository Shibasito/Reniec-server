#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
client_load_test.py
Genera carga contra el bank-server vía RabbitMQ (RPC) creando clientes (Register),
haciendo depósitos (Deposit) y solicitando préstamos (CreateLoan).
Ajustado para usar DNIs que EXISTEN en el RENIEC (sembrados por reniec_server.py).

Requisitos:
    pip install pika==1.3.2
    (opcional, para gráficos) matplotlib

Vars de entorno (con defaults razonables):
    RABBIT_HOST=host.docker.internal
    RABBIT_PORT=5672
    RABBIT_USERNAME=admin
    RABBIT_PASSWORD=admin
    RABBIT_VHOST=/
    RABBIT_EXCHANGE=rabbit_exchange
    BANK_ROUTING=bank_operation

    WORKERS=20
    CLIENTS_PER_WORKER=60
    TX_PER_CLIENT=2
    LOANS_PER_CLIENT=1

    # Dónde guardar resultados
    CSV_PATH=                # p.ej. /out/results.csv
    PLOT_DIR=                # p.ej. /out
    PLOT_PREFIX=run1

    # Rango de DNIs sembrados en RENIEC (debe coincidir con reniec_server.py)
    RENIEC_SEED_BASE=10000000
    RENIEC_SEED_COUNT=10000

    # Opcional: usar lista fija de DNIs “humanos” además del rango masivo
    USE_FIXED_DNIS=false
"""

import json
import os
import random
import string
import threading
import time
from collections import defaultdict
from statistics import median

import pika

# ------------------ Config ------------------
RABBIT_HOST = os.getenv("RABBIT_HOST", "host.docker.internal")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USERNAME = os.getenv("RABBIT_USERNAME", "admin")
RABBIT_PASSWORD = os.getenv("RABBIT_PASSWORD", "admin")
RABBIT_VHOST = os.getenv("RABBIT_VHOST", "/")
RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "rabbit_exchange")
BANK_ROUTING = os.getenv("BANK_ROUTING", "bank_operation")

WORKERS = int(os.getenv("WORKERS", "20"))
CLIENTS_PER_WORKER = int(os.getenv("CLIENTS_PER_WORKER", "60"))
TX_PER_CLIENT = int(os.getenv("TX_PER_CLIENT", "2"))
LOANS_PER_CLIENT = int(os.getenv("LOANS_PER_CLIENT", "1"))

CSV_PATH = os.getenv("CSV_PATH")
PLOT_DIR = os.getenv("PLOT_DIR")
PLOT_PREFIX = os.getenv("PLOT_PREFIX", "load")

# DNIs sembrados en RENIEC
SEED_BASE = int(os.getenv("RENIEC_SEED_BASE", "10000000"))
SEED_COUNT = int(os.getenv("RENIEC_SEED_COUNT", "10000"))
USE_FIXED_DNIS = os.getenv("USE_FIXED_DNIS", "false").lower() in ("1", "true", "yes")

# Lista de DNIs “humanos” (los 7 de la demo + algunos extras válidos)
FIXED_DNIS = [
    "12345678", "23456789", "34567890", "45678901", "56789012", "67890123", "78901234"
]


# ------------------ Utilidades ------------------
def rand_id(prefix: str, n: int = 6) -> str:
    return f"{prefix}-{''.join(random.choices(string.hexdigits.lower(), k=n))}"

def percentiles(nums, ps=(50, 95, 99)):
    if not nums:
        return {p: None for p in ps}
    xs = sorted(nums)
    out = {}
    for p in ps:
        k = int(round((p / 100.0) * (len(xs) - 1)))
        out[p] = xs[k]
    return out

def pick_dni(wid: int, i: int) -> str:
    """
    Devuelve un DNI que existe en RENIEC:
      - Si USE_FIXED_DNIS: alterna algunos de la lista fija (7) para mezclar.
      - En general: toma uno del rango [SEED_BASE, SEED_BASE + SEED_COUNT - 1],
        distribuido de forma determinista por (worker,index) para evitar colisiones fuertes.
    """
    # 1) a veces usa uno fijo (si se activó)
    if USE_FIXED_DNIS and ((wid + i) % 8 == 0):
        return FIXED_DNIS[(wid + i) % len(FIXED_DNIS)]
    # 2) rango masivo sembrado
    idx = (wid * CLIENTS_PER_WORKER + i) % SEED_COUNT
    return f"{SEED_BASE + idx:08d}"


# ------------------ Plantillas de mensajes ------------------
def build_register(dni: str, initial_saldo: float = 0.0):
    # Register crea cliente + cuenta y valida con RENIEC
    return {
        "type": "Register",
        "messageId": rand_id("m"),
        "dni": dni,
        "password": "test123",
        "nombres": "TEST NOMBRE",
        "apellidoPat": "APELLIDO1",
        "apellidoMat": "APELLIDO2",
        "saldo": float(initial_saldo)
    }

def build_deposit(account_id: str, amount: float):
    return {
        "type": "Deposit",
        "messageId": rand_id("m"),
        "accountId": account_id,
        "amount": float(amount),
    }

def build_create_loan(client_id: str, account_id: str, principal: float):
    # El banco acepta float o string; aquí usamos float
    return {
        "type": "CreateLoan",
        "messageId": rand_id("m"),
        "clientId": client_id,
        "accountId": account_id,
        "principal": float(principal),
    }


# ------------------ Worker (RPC simple con callback exclusivo) ------------------
class Worker(threading.Thread):
    def __init__(self, wid: int, metrics: dict):
        super().__init__(daemon=True)
        self.wid = wid
        self.metrics = metrics
        self.stop_event = threading.Event()   # <- antes era self._stop

        creds = pika.PlainCredentials(RABBIT_USERNAME, RABBIT_PASSWORD)
        params = pika.ConnectionParameters(
            host=RABBIT_HOST,
            port=RABBIT_PORT,
            virtual_host=RABBIT_VHOST,
            credentials=creds,
            heartbeat=30,
            blocked_connection_timeout=60,
            client_properties={"connection_name": f"load-client-{wid}"},
        )
        self.conn = pika.BlockingConnection(params)
        self.ch = self.conn.channel()
        self.ch.exchange_declare(exchange=RABBIT_EXCHANGE, exchange_type="direct", durable=True)

        result = self.ch.queue_declare(queue="", exclusive=True, auto_delete=True)
        self.callback_queue = result.method.queue

        self.responses = {}
        self.lock = threading.Lock()

        def on_response(ch, method, props, body):
            cid = props.correlation_id
            with self.lock:
                self.responses[cid] = body
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.ch.basic_consume(queue=self.callback_queue, on_message_callback=on_response, auto_ack=False)

    def rpc_call(self, body_dict: dict) -> tuple[bool, float, dict]:
        cid = rand_id("corr", 10)
        props = pika.BasicProperties(
            reply_to=self.callback_queue,
            correlation_id=cid,
            content_type="application/json",
        )
        payload = json.dumps(body_dict).encode("utf-8")
        t0 = time.perf_counter()
        self.ch.basic_publish(
            exchange=RABBIT_EXCHANGE,
            routing_key=BANK_ROUTING,
            properties=props,
            body=payload,
        )
        while True:
            self.conn.process_data_events(time_limit=0.05)
            with self.lock:
                if cid in self.responses:
                    raw = self.responses.pop(cid)
                    break
            if self.stop_event.is_set():      # <- antes: self._stop.is_set()
                return False, 0.0, {}
        dt = time.perf_counter() - t0
        try:
            resp = json.loads(raw.decode("utf-8"))
        except Exception:
            resp = {"ok": False, "error": {"message": "decode_error"}}
        ok = bool(resp.get("ok", False))
        return ok, dt, resp

    def run(self):
        for i in range(CLIENTS_PER_WORKER):
            if self.stop_event.is_set():      
                break

            dni = pick_dni(self.wid, i)
            ok, dt, resp = self.rpc_call(build_register(dni, initial_saldo=0))
            self._record("Register", ok, dt)
            if not ok:
                continue

            data = (resp or {}).get("data") or {}
            client_id = data.get("clientId") or data.get("clienteId")
            account_id = data.get("accountId")
            if not client_id or not account_id:
                self._record("RegisterParse", False, 0.0)
                continue

            for _ in range(TX_PER_CLIENT):
                amount = random.choice([50, 75, 100, 150, 200, 250])
                ok, dt, _ = self.rpc_call(build_deposit(account_id, amount))
                self._record("Deposit", ok, dt)

            for _ in range(LOANS_PER_CLIENT):
                principal = random.choice([200, 300, 400, 500])
                ok, dt, _ = self.rpc_call(build_create_loan(client_id, account_id, principal))
                self._record("CreateLoan", ok, dt)

        try:
            self.ch.close()
        finally:
            self.conn.close()

    def _record(self, kind: str, ok: bool, dt: float):
        ms = dt * 1000.0
        with self.metrics["lock"]:
            self.metrics["total"] += 1
            self.metrics["by_kind"][kind]["count"] += 1
            self.metrics["by_kind"][kind]["ok"] += int(ok)
            self.metrics["by_kind"][kind]["lat"].append(ms)
            if not ok:
                self.metrics["fail"] += 1

    def stop(self):
        self.stop_event.set()                 


# ------------------ Reporte / Plots ------------------
try:
    import matplotlib
    matplotlib.use("Agg")  # headless
    import matplotlib.pyplot as plt
    _HAS_MPL = True
except Exception:
    _HAS_MPL = False

def save_plots(metrics: dict):
    if not PLOT_DIR or not _HAS_MPL:
        print("[PLOT] Saltando: PLOT_DIR no definido o matplotlib no disponible")
        return
    os.makedirs(PLOT_DIR, exist_ok=True)

    # Histograma de latencias global
    all_lat = []
    for v in metrics["by_kind"].values():
        all_lat.extend(v["lat"])
    if all_lat:
        plt.figure()
        plt.hist(all_lat, bins=40)
        plt.xlabel("Latencia (ms)")
        plt.ylabel("Frecuencia")
        plt.title("Distribución de latencia (total)")
        plt.tight_layout()
        plt.savefig(os.path.join(PLOT_DIR, f"{PLOT_PREFIX}_lat_hist.png"))
        plt.close()

    # p50/p95/p99 y tasa de OK por operación
    kinds = []
    p50s, p95s, p99s, oks = [], [], [], []
    for kind, v in sorted(metrics["by_kind"].items()):
        kinds.append(kind)
        lat = sorted(v["lat"])
        def pick(p):
            if not lat:
                return 0.0
            k = int(round((p / 100.0) * (len(lat) - 1)))
            return lat[k]
        p50s.append(pick(50))
        p95s.append(pick(95))
        p99s.append(pick(99))
        okr = (100.0 * v["ok"] / v["count"]) if v["count"] else 0.0
        oks.append(okr)

    if kinds:
        x = range(len(kinds))
        # p50/p95/p99 apilados
        plt.figure()
        plt.bar(x, p50s, label="p50")
        plt.bar(x, [p95s[i] - p50s[i] for i in x], bottom=p50s, label="p95-extra")
        plt.bar(x, [max(0, p99s[i] - p95s[i]) for i in x],
                bottom=p95s, label="p99-extra")
        plt.xticks(list(x), kinds, rotation=20)
        plt.ylabel("ms")
        plt.title("Latencias por operación")
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(PLOT_DIR, f"{PLOT_PREFIX}_lat_by_op.png"))
        plt.close()

        # %OK por operación
        plt.figure()
        plt.bar(x, oks)
        plt.xticks(list(x), kinds, rotation=20)
        plt.ylabel("OK (%)")
        plt.ylim(0, 100)
        plt.title("Tasa de éxito por operación")
        plt.tight_layout()
        plt.savefig(os.path.join(PLOT_DIR, f"{PLOT_PREFIX}_ok_by_op.png"))
        plt.close()


# ------------------ Main ------------------
def main():
    random.seed(1337)
    metrics = {
        "start": time.time(),
        "total": 0,
        "fail": 0,
        "by_kind": defaultdict(lambda: {"count": 0, "ok": 0, "lat": []}),
        "lock": threading.Lock(),
    }

    print(f"[CFG] workers={WORKERS} clients/worker={CLIENTS_PER_WORKER} "
          f"tx/client={TX_PER_CLIENT} loans/client={LOANS_PER_CLIENT}")
    print(f"[CFG] RENIEC seed base={SEED_BASE} count={SEED_COUNT} fixed={USE_FIXED_DNIS}")

    workers = [Worker(w, metrics) for w in range(WORKERS)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    elapsed = time.time() - metrics["start"]
    total = metrics["total"]
    fail = metrics["fail"]
    ok = total - fail
    tps = total / elapsed if elapsed > 0 else 0.0

    print("\n===== RESULTADOS =====")
    print(f"Mensajes totales  : {total}")
    print(f"Éxitos / Fallos    : {ok} / {fail}")
    print(f"Tiempo total (s)   : {elapsed:.2f}")
    print(f"Throughput (msg/s) : {tps:.1f}")

    # Latencias agregadas
    all_lat = []
    for v in metrics["by_kind"].values():
        all_lat.extend(v["lat"])
    p = percentiles(all_lat) if all_lat else {}
    print("\nLatencias totales (ms):", end=" ")
    if p:
        print(f"p50={p[50]:.1f}  p95={p[95]:.1f}  p99={p[99]:.1f}")
    else:
        print("sin datos")

    print("\nPor operación:")
    for kind, v in sorted(metrics["by_kind"].items()):
        pc = percentiles(v["lat"]) if v["lat"] else {}
        okr = (100.0 * v["ok"] / v["count"]) if v["count"] else 0.0
        if pc:
            print(f" - {kind:<18} n={v['count']:<5} ok%={okr:5.1f}  "
                  f"p50={pc[50]:5.1f}  p95={pc[95]:5.1f}  p99={pc[99]:5.1f}")
        else:
            print(f" - {kind:<18} n={v['count']:<5} ok%={okr:5.1f}")

    # CSV opcional
    if CSV_PATH:
        try:
            import csv
            with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["kind", "count", "ok", "p50_ms", "p95_ms", "p99_ms"])
                for kind, v in sorted(metrics["by_kind"].items()):
                    pc = percentiles(v["lat"]) if v["lat"] else {50: None, 95: None, 99: None}
                    w.writerow([kind, v["count"], v["ok"], pc[50], pc[95], pc[99]])
            print(f"\nCSV escrito en {CSV_PATH}")
        except Exception as e:
            print(f"\nNo se pudo escribir CSV: {e}")

    save_plots(metrics)


if __name__ == "__main__":
    main()

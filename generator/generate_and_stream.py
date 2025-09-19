from __future__ import annotations
import os, io, csv, json, time, uuid, random, logging, threading, sys
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import psycopg2
import psycopg2.extras
from faker import Faker

# Optional Parquet export support
try:
    import pyarrow as pa, pyarrow.parquet as pq
except Exception:
    pa = None
    pq = None

# Optional high-performance JSON serializer
try:
    import orjson as _orjson
    def dumps_json(o): return _orjson.dumps(o).decode()
except Exception:
    def dumps_json(o): return json.dumps(o, default=str)

# Prometheus metrics client
from prometheus_client import Counter, Histogram, Gauge, start_http_server, Info

# -----------------------------------------------------------------------------
# Logging configuration
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger("generator")

# -----------------------------------------------------------------------------
# Environment-driven configuration (good for container/K8s deployments)
# -----------------------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST", os.getenv("POSTGRES_HOST", "db"))
PG_PORT = int(os.getenv("PG_PORT", os.getenv("POSTGRES_PORT", "5432")))
PG_DB = os.getenv("PG_DB", os.getenv("POSTGRES_DB", "loanfintech"))
PG_USER = os.getenv("PG_USER", os.getenv("POSTGRES_USER", "loan_admin"))
PG_PASSWORD = os.getenv("PG_PASSWORD", os.getenv("POSTGRES_PASSWORD", "loan_pass"))

# Workload sizing
TOTAL_TRANSACTIONS = int(os.getenv("TRANSACTIONS", "1000000"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100000"))
LOANS_COUNT = int(os.getenv("LOANS_COUNT", "1000"))
USERS_COUNT = int(os.getenv("USERS_COUNT", "12"))
CUSTOMERS_COUNT = int(os.getenv("CUSTOMERS_COUNT", "1000"))

# Parquet export toggles
PARQUET_ENABLED = bool(int(os.getenv("PARQUET_ENABLED", "1")))
PARQUET_DIR = os.getenv("PARQUET_DIR", "/output/parquet")
PARQUET_COMPRESSION = os.getenv("PARQUET_COMPRESSION", "snappy")

# Global RNG seed for reproducibility
SEED = int(os.getenv("SEED", "42"))

# Optional chunked/parallel processing: set CHUNK_INDEX/CHUNKS_TOTAL in env
CHUNK_INDEX_ENV = os.getenv("CHUNK_INDEX", None)
CHUNKS_TOTAL_ENV = os.getenv("CHUNKS_TOTAL", None)
CHUNK_INDEX = int(CHUNK_INDEX_ENV) if CHUNK_INDEX_ENV is not None else None
CHUNKS_TOTAL = int(CHUNKS_TOTAL_ENV) if CHUNKS_TOTAL_ENV is not None else None

# Prometheus metrics HTTP endpoint config
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
METRICS_LOG_INTERVAL = int(os.getenv("METRICS_LOG_INTERVAL", "10"))  # seconds

# Faker and RNG instances
F = Faker()
F.seed_instance(SEED)
RND_MASTER = random.Random(SEED + 999)

# Guard: Parquet requested but pyarrow missing
if PARQUET_ENABLED and (pa is None or pq is None):
    raise RuntimeError("Parquet requested but pyarrow not installed.")

# -----------------------------------------------------------------------------
# Prometheus metrics declarations
# -----------------------------------------------------------------------------
INFO = Info('generator', 'Loan fintech data generator info')
INFO.info({'seed': str(SEED), 'batch_size': str(BATCH_SIZE)})

TX_GENERATED = Counter('transactions_generated_total', 'Rows generated (pre-DB)')
CHUNKS_PROCESSED = Counter('chunks_processed_total', 'Chunks successfully streamed')
TX_STREAMED = Counter('transactions_streamed_total', 'Rows committed to Postgres')
PARQUET_CHUNKS = Counter('parquet_chunks_written_total', 'Parquet chunk files written')
GENERATION_ERRORS = Counter('generation_errors_total', 'Errors during generation/streaming')
CHUNK_DURATION = Histogram('chunk_generation_seconds', 'Chunk processing time')
LAST_CHUNK_DURATION = Gauge('last_chunk_duration_seconds', 'Duration of last chunk')
IN_PROGRESS_GAUGE = Gauge('transactions_in_progress', 'Rows currently being streamed')

# -----------------------------------------------------------------------------
# Helper utilities
# -----------------------------------------------------------------------------
def uid() -> str:
    """Generate UUID4 string."""
    return str(uuid.uuid4())

def dt_iso(offset_days=0):
    """UTC ISO timestamp with optional days offset into the past."""
    return (datetime.utcnow() - timedelta(days=offset_days)).isoformat()

def cents(x: float) -> int:
    """Convert float dollars to integer cents with HALF_UP rounding."""
    return int(Decimal(x).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) * 100)

def dsn():
    """Assemble PostgreSQL DSN string."""
    return f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASSWORD}"

def wait_for_db(timeout=120):
    """Block until Postgres is accepting connections or timeout."""
    start = time.time()
    while True:
        try:
            conn = psycopg2.connect(dsn())
            conn.close()
            logger.info("Postgres is available.")
            return
        except Exception as e:
            if time.time() - start > timeout:
                logger.exception("Timed out waiting for Postgres.")
                raise
            logger.debug("Waiting for Postgres (%s)...", e)
            time.sleep(1)

def open_conn():
    """Open a psycopg2 connection with autocommit disabled."""
    conn = psycopg2.connect(dsn())
    conn.autocommit = False
    return conn

# -----------------------------------------------------------------------------
# Background metrics logging thread: logs delta + rate for easy monitoring
# -----------------------------------------------------------------------------
class MetricsLogger(threading.Thread):
    def __init__(self, interval: int = METRICS_LOG_INTERVAL):
        super().__init__(daemon=True)
        self.interval = interval
        self._stop = threading.Event()
        self._last_tx_count = 0
        self._last_time = time.time()

    def stop(self):
        self._stop.set()

    def run(self):
        logger.info("MetricsLogger started, logging every %ds", self.interval)
        while not self._stop.is_set():
            time.sleep(self.interval)
            try:
                # Compute rate of transactions streamed/sec
                now = time.time()
                tx_count = int(TX_STREAMED._value.get()) if hasattr(TX_STREAMED, "_value") else 0
                if tx_count == 0:  # fallback for non-private attr access
                    try:
                        tx_count = list(TX_STREAMED.collect())[0].samples[0].value
                    except Exception:
                        tx_count = 0
                delta = tx_count - self._last_tx_count
                dt = now - self._last_time or 1.0
                rate = delta / dt
                logger.info(
                    "metrics snapshot: tx_streamed_total=%d delta=%d rate=%.2f/sec "
                    "chunks_processed=%d parquet_chunks=%d errors=%d",
                    tx_count, delta, rate,
                    int(CHUNKS_PROCESSED._value.get()) if hasattr(CHUNKS_PROCESSED, "_value") else 0,
                    int(PARQUET_CHUNKS._value.get()) if hasattr(PARQUET_CHUNKS, "_value") else 0,
                    int(GENERATION_ERRORS._value.get()) if hasattr(GENERATION_ERRORS, "_value") else 0
                )
                self._last_tx_count = tx_count
                self._last_time = now
            except Exception:
                logger.exception("MetricsLogger error (continuing)")

# -----------------------------------------------------------------------------
# Data insert helpers: bulk COPY for high throughput
# -----------------------------------------------------------------------------
def insert_users(conn, count, seed) -> List[Dict]:
    """Generate and bulk-insert fake users."""
    logger.info("Generating %d users...", count)
    rnd = random.Random(seed + 1)
    faker = Faker(); faker.seed_instance(seed + 1)
    users = []
    roles = ["admin","ops","underwriter","support","investor"]
    for _ in range(count):
        users.append({
            "id": uid(),
            "email": faker.unique.email(),
            "password_hash": faker.sha1(),
            "role": rnd.choice(roles),
            "name_first": faker.first_name(),
            "name_last": faker.last_name(),
            "status": "active",
            "created_at": dt_iso(rnd.randint(0,900)),
            "updated_at": dt_iso(0),
            "last_login_at": dt_iso(rnd.randint(0,60))
        })
    # COPY is fastest for bulk insert
    cols = ["id","email","password_hash","role","name_first","name_last","status","created_at","updated_at","last_login_at"]
    sio = io.StringIO()
    w = csv.writer(sio)
    for u in users:
        w.writerow([u[c] for c in cols])
    sio.seek(0)
    cur = conn.cursor()
    cur.copy_expert(f"COPY users ({', '.join(cols)}) FROM STDIN WITH CSV", sio)
    conn.commit()
    logger.info("Inserted %d users.", len(users))
    return users

def insert_customers(conn, count, seed) -> List[Dict]:
    """Generate and bulk-insert fake customers."""
    logger.info("Generating %d customers...", count)
    rnd = random.Random(seed + 2)
    faker = Faker(); faker.seed_instance(seed + 2)
    customers = []
    for i in range(count):
        customers.append({
            "id": uid(),
            "external_id": f"CUST-{100000+i}",
            "email": faker.safe_email(),
            "phone": faker.phone_number(),
            "name_first": faker.first_name(),
            "name_last": faker.last_name(),
            "dob": faker.date_of_birth(minimum_age=21, maximum_age=80).isoformat(),
            "ssn_hash": faker.sha1(),
            "preferred_contact": json.dumps({"method": rnd.choice(["email","sms","phone"])}),
            "metadata": json.dumps({"source": rnd.choice(["web","partner","mobile"])}),
            "created_at": dt_iso(rnd.randint(0,900)),
            "updated_at": dt_iso(0)
        })
    cols = ["id","external_id","email","phone","name_first","name_last","dob","ssn_hash",
            "preferred_contact","metadata","created_at","updated_at"]
    sio = io.StringIO(); w = csv.writer(sio)
    for c in customers:
        w.writerow([c.get(col,"") for col in cols])
    sio.seek(0)
    cur = conn.cursor()
    cur.copy_expert(f"COPY customers ({', '.join(cols)}) FROM STDIN WITH CSV", sio)
    conn.commit()
    logger.info("Inserted %d customers.", len(customers))
    return customers

def insert_loans(conn, customers, count, seed) -> List[Dict]:
    """Generate and bulk-insert fake loans."""
    logger.info("Generating %d loans...", count)
    rnd = random.Random(seed + 3)
    faker = Faker(); faker.seed_instance(seed + 3)
    loans = []
    for _ in range(count):
        cust = rnd.choice(customers)
        principal = cents(rnd.choice([1000,2000,3000,5000,10000]))
        term = rnd.choice([12,24,36,48])
        rate = round(rnd.uniform(6.0, 29.0), 2)
        disbursed = dt_iso(rnd.randint(0,400))
        loans.append({
            "id": uid(),
            "loan_number": f"LN-{uuid.uuid4().hex[:10].upper()}",
            "customer_id": cust["id"],
            "offer_id": None,
            "principal_cents": principal,
            "outstanding_balance_cents": principal,
            "term_months": term,
            "interest_rate_pct": rate,
            "status": "active",
            "funding_model": rnd.choice(["balance_sheet","marketplace"]),
            "disbursed_at": disbursed,
            "maturity_date": (datetime.utcnow().date()).isoformat(),
            "next_payment_due_date": dt_iso(30),
            "created_at": disbursed,
            "updated_at": dt_iso(0)
        })
    cols = ["id","loan_number","customer_id","offer_id","principal_cents","outstanding_balance_cents",
            "term_months","interest_rate_pct","status","funding_model","disbursed_at",
            "maturity_date","next_payment_due_date","created_at","updated_at"]
    sio = io.StringIO(); w = csv.writer(sio)
    for L in loans:
        w.writerow([L.get(c,"") if L.get(c) is not None else "" for c in cols])
    sio.seek(0)
    cur = conn.cursor()
    cur.copy_expert(f"COPY loans ({', '.join(cols)}) FROM STDIN WITH CSV", sio)
    conn.commit()
    logger.info("Inserted %d loans.", len(loans))
    return loans

# -----------------------------------------------------------------------------
# Transaction generation & streaming
# -----------------------------------------------------------------------------
def generate_chunk_rows(chunk_seed, chunk_size, loans_sample) -> List[List]:
    """Create a list of fake transaction rows for a chunk."""
    rnd = random.Random(chunk_seed)
    faker = Faker(); faker.seed_instance(chunk_seed)
    rows = []
    for _ in range(chunk_size):
        loan = rnd.choice(loans_sample)
        amount = rnd.randint(100,20000)
        rows.append([
            uid(),
            loan["id"],
            rnd.choice(["platform_funds","investor_pool","customer_card","customer_ach"]),
            rnd.choice(["customer_ach","platform_funds","investor_pool"]),
            amount,
            rnd.choice(["repayment","disbursement","fee","refund","chargeback","investment_funding"]),
            rnd.choice(["completed","pending","failed"]),
            f"ref_{uuid.uuid4().hex[:12]}",
            dumps_json({"meta": rnd.randint(1,9999)}),
            dt_iso(rnd.randint(0,400))
        ])
    TX_GENERATED.inc(len(rows))
    return rows

def stream_transactions(conn, loans_sample, total, batch_size, seed,
                        parquet_enabled, parquet_dir, chunk_index=None, chunks_total=None):
    """
    Stream transactions in chunks into Postgres and optionally Parquet.
    Supports multi-process parallelism by chunk-index assignment.
    """
    tx_cols = ["id","loan_id","origin_account","destination_account","amount_cents",
               "type","status","reference","metadata","created_at"]
    total_chunks = (total + batch_size - 1) // batch_size

    # Determine which chunk indexes this process owns
    if chunk_index is None or chunks_total is None:
        assigned_chunk_indices = list(range(total_chunks))
        logger.info("No CHUNK_INDEX/CHUNKS_TOTAL specified: processing all %d chunks.", total_chunks)
    else:
        assigned_chunk_indices = [i for i in range(total_chunks) if (i % chunks_total) == chunk_index]
        logger.info("CHUNK_INDEX=%d CHUNKS_TOTAL=%d -> assigned %d chunks out of %d.",
                    chunk_index, chunks_total, len(assigned_chunk_indices), total_chunks)

    if parquet_enabled and parquet_dir:
        os.makedirs(parquet_dir, exist_ok=True)

    written = 0
    start = time.time()
    for idx in assigned_chunk_indices:
        chunk_start = time.time()
        chunk_seed = seed + 10000 + idx
        # Last chunk may be smaller
        to_write = batch_size if idx < total_chunks - 1 else (total - batch_size * (total_chunks - 1))
        logger.info("Processing chunk idx=%d rows=%d seed=%d", idx, to_write, chunk_seed)

        try:
            IN_PROGRESS_GAUGE.inc(to_write)
            rows = generate_chunk_rows(chunk_seed, to_write, loans_sample)

            # Stream into Postgres via COPY for max throughput
            sio = io.StringIO(); w = csv.writer(sio)
            for r in rows: w.writerow(r)
            sio.seek(0)
            cur = conn.cursor()
            cur.copy_expert(f"COPY transactions ({', '.join(tx_cols)}) FROM STDIN WITH CSV", sio)
            conn.commit()

            # Metrics update
            CHUNKS_PROCESSED.inc(1)
            TX_STREAMED.inc(len(rows))
            chunk_dur = time.time() - chunk_start
            CHUNK_DURATION.observe(chunk_dur)
            LAST_CHUNK_DURATION.set(chunk_dur)
            logger.info("Chunk %d streamed (rows=%d, duration=%.2fs).", idx, len(rows), chunk_dur)

            # Optional Parquet output
            if parquet_enabled and parquet_dir:
                try:
                    table = pa.Table.from_pydict({
                        "id":[r[0] for r in rows],
                        "loan_id":[r[1] for r in rows],
                        "origin_account":[r[2] for r in rows],
                        "destination_account":[r[3] for r in rows],
                        "amount_cents":[r[4] for r in rows],
                        "type":[r[5] for r in rows],
                        "status":[r[6] for r in rows],
                        "reference":[r[7] for r in rows],
                        "metadata":[json.loads(r[8]) for r in rows],
                        "created_at":[r[9] for r in rows]
                    })
                    tmp_path = os.path.join(parquet_dir, f"transactions_part_{idx:06d}.parquet.tmp")
                    final_path = os.path.join(parquet_dir, f"transactions_part_{idx:06d}.parquet")
                    pq.write_table(table, tmp_path, compression=PARQUET_COMPRESSION)
                    os.replace(tmp_path, final_path)  # atomic rename
                    PARQUET_CHUNKS.inc(1)
                    logger.info("Wrote Parquet chunk: %s", final_path)
                except Exception:
                    logger.exception("Parquet write failed for chunk %d", idx)
                    raise

        except Exception:
            # Roll back and surface errors for monitoring
            GENERATION_ERRORS.inc(1)
            conn.rollback()
            logger.exception("Error processing chunk %d, aborting this instance.", idx)
            raise
        finally:
            IN_PROGRESS_GAUGE.dec(to_write)

        written += to_write
        elapsed = time.time() - start
        logger.info("Instance progress: %d/%d rows written (elapsed=%.1fs)", written, total, elapsed)

    total_elapsed = time.time() - start
    logger.info("Instance complete. Rows written: %d (elapsed=%.1fs)", written, total_elapsed)

# -----------------------------------------------------------------------------
# Main orchestration
# -----------------------------------------------------------------------------
def main():
    logger.info("Starting generator: total_transactions=%d batch_size=%d", TOTAL_TRANSACTIONS, BATCH_SIZE)

    # Start Prometheus endpoint and metrics logger
    start_http_server(METRICS_PORT)
    metrics_thread = MetricsLogger(interval=METRICS_LOG_INTERVAL)
    metrics_thread.start()

    try:
        wait_for_db()
        conn = open_conn()

        # Seed base tables (idempotent)
        users = insert_users(conn, USERS_COUNT, SEED)
        customers = insert_customers(conn, CUSTOMERS_COUNT, SEED)
        loans = insert_loans(conn, customers, LOANS_COUNT, SEED)

        # Stream transactions (multi-process safe)
        stream_transactions(
            conn, loans, TOTAL_TRANSACTIONS, BATCH_SIZE, SEED,
            PARQUET_ENABLED, PARQUET_DIR,
            chunk_index=CHUNK_INDEX, chunks_total=CHUNKS_TOTAL
        )

        logger.info("Generation finished successfully.")
    except Exception:
        logger.exception("Fatal error in generator.")
        sys.exit(1)
    finally:
        metrics_thread.stop()
        logger.info("MetricsLogger stopped.")

if __name__ == "__main__":
    main()

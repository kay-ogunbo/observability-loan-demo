# Loan Fintech Data Generator (Docker Compose)

Synthetic data generator for a loan fintech platform.
Populates PostgreSQL with users, customers, loans, and transactions, with optional Parquet export and Prometheus metrics.

---

## Features

- Generates **users, customers, loans**, and **transactions**.
- Bulk inserts via PostgreSQL `COPY` for high throughput.
- Optional **Parquet export** of transaction data.
- Prometheus metrics exposed for observability.
- Fully **Dockerized** with Docker Compose support.
- Supports **parallel/chunked processing**.

---

## Requirements

- Docker 20+
- Docker Compose 1.29+
- A PostgreSQL service (included in the Compose setup below)
- Optional: `pyarrow` for Parquet export
- Optional: `orjson` for faster JSON serialization

---

## Getting Started

1. **Clone the repository**

```bash
git clone https://github.com/<username>/observability-loan-demo.git
cd observability-loan-demo
docker compose up --build -d  OR docker-compose up --build -d

The generator service waits for the db service and starts generating data.
Prometheus metrics are available at http://localhost:9090/
Grafana metrics are available at http://localhost:3001/


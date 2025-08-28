# Data Pipeline for Quick Commerce Apps

An end‑to‑end pipeline that ingests **batch** (MySQL, MongoDB) and **real‑time** (Kafka) data,
processes it via **Pandas/Spark**, and exposes **monitoring** with Prometheus + Grafana.
This scaffold is generated from your project brief and uses your Excalidraw file as the
architecture reference (place it under `docs/architecture/` to keep it with the code).

## Quick Start (Local, via Docker Compose)

**Prereqs:** Docker & docker-compose installed.

```bash
# 1) Start core infra
docker compose up -d zookeeper kafka mysql mongo prometheus grafana

# 2) Initialize Airflow (first run only)
docker compose up airflow-init

# 3) Start Airflow & Spark
docker compose up -d airflow-webserver airflow-scheduler spark-master spark-worker
```

- Airflow UI: http://localhost:8080  (user: `airflow`, pwd: `airflow`)
- Grafana: http://localhost:3000      (user: `admin`,   pwd: `admin`)
- Prometheus: http://localhost:9090
- Kafka broker: `kafka:9092`
- MySQL: `mysql:3306` (user: `root` / password: `rootpass`)
- MongoDB: `mongo:27017`

> **Note:** For a simple local demo, the batch job writes cleaned data to `data/warehouse/` as Parquet.
> You can later switch the `processing/spark_batch_job.py` sink to Snowflake/Redshift by editing
> `config/app_config.yaml` and adding the relevant connector packages to the Spark image.

## Using Your Excalidraw Diagram

Place your Excalidraw file here (so it’s versioned with the repo):

```
docs/architecture/Capstone_Project.excalidraw
```

We reference it in `docs/ARCHITECTURE.md`. If you export a PNG, add it to the same folder.

## Airflow DAG

The DAG `quickcommerce_dag` runs:

1. `export_orders` (MySQL → CSV)
2. `export_inventory` (MongoDB → JSON)
3. `spark_batch_transform` (Spark job → Parquet in `data/warehouse/`)

You can schedule it hourly/daily. A streaming job (`spark_streaming_job.py`) is provided for
Kafka topics `gps_stream` and `status_stream` (submit separately with `spark-submit`).

## Project Layout

```
quick-commerce-pipeline/
├── airflow_dags/quickcommerce_dag.py
├── config/app_config.yaml
├── data/                 # runtime outputs (mounted volume)
├── docker-compose.yml
├── docs/
│   ├── ARCHITECTURE.md
│   └── architecture/     # put your Excalidraw here
├── ingestion/
│   ├── batch/
│   │   ├── export_inventory.py
│   │   └── export_orders.py
│   └── streaming/
│       ├── gps_simulator.py
│       └── producer.py
├── monitoring/
│   ├── grafana_dashboards.json
│   └── prometheus.yml
├── processing/
│   ├── spark_batch_job.py
│   └── spark_streaming_job.py
└── samples/
    ├── inventory_sample.json
    └── orders_sample.csv
```

## Run the Kafka Producer (simulator)

In another terminal:

```bash
docker compose exec -it spark-master bash
# Inside the container, you can also run spark-submit for streaming job.
exit

# Produce gps/status events from host (uses kafka-python)
python3 ingestion/streaming/producer.py
```

## Monitoring

- Prometheus scrapes Airflow & Docker host metrics.
- Grafana has a starter dashboard at provisioning import (`monitoring/grafana_dashboards.json`).

## Next Steps

- Wire Snowflake/Redshift sink in Spark (or dbt on top of Parquet).
- Expand Grafana panels (data freshness, throughput, SLA).
- Add CI checks (flake8, black) and unit tests for transforms.

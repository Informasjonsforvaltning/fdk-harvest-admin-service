# MongoDB → PostgreSQL migration

Migrates documents from the old **fdk-harvest-admin** (MongoDB) into **fdk-harvest-admin-service** (PostgreSQL).

- **dataSources** → **data_sources** (upsert by id)
- **reports** → **harvest_runs** (one row per data source + data type; new `run_id` UUIDs)

Designed to run locally with port-forwards. All connection URLs are configurable via environment or CLI.

## Prerequisites

- Python 3.10+
- Port-forwards to MongoDB and PostgreSQL (see below)

## Setup

```bash
cd scripts/migrate-mongo-to-postgres
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Port-forwards (examples)

Forward to your local ports, then **use `localhost`** in MONGO_HOST / MONGO_URI. In-cluster hostnames (e.g. `*.svc.cluster.local`) only resolve inside the cluster, so the script would fail with "nodename nor servname provided, or not known". When using component vars, the script adds `directConnection=true` so the driver uses only the port-forwarded host (required for replica sets).

```bash
# MongoDB (e.g. from staging)
kubectl port-forward -n staging svc/staging-mongodb-staging 27017:27017

# PostgreSQL (fdk-harvest-admin-service DB)
kubectl port-forward -n <namespace> svc/<postgres-svc> 5432:5432
```

## Configuration

Set connection URLs via environment or a `.env` file in this directory. `run-migrate.sh` sources `.env` automatically if present. Copy from the example:

```bash
cp .env.example .env
# Edit .env with your credentials (e.g. after port-forward)
```

**Passwords:** Use **component env vars** (e.g. `MONGO_PASSWORD`, `POSTGRES_PASSWORD`) so the script can URL-encode them for you—required when passwords contain `@`, `#`, `%`, etc. If you set full `MONGO_URI`/`POSTGRES_URI` instead, encode special characters yourself (e.g. `@` → `%40`).

| Variable / flag       | Default (if unset) | Description |
|-----------------------|--------------------|-------------|
| `MONGO_URI` / `--mongo-uri` | (see below) | Full MongoDB URI. Overridden when `MONGO_PASSWORD` is set. |
| `MONGO_USER`, `MONGO_PASSWORD`, `MONGO_HOST`, `MONGO_DATABASE`, `MONGO_AUTH_SOURCE` | root, -, localhost:27017, fdkHarvestAdmin, admin | Used to build URI with encoded password when `MONGO_PASSWORD` is set. |
| `POSTGRES_URI` / `--postgres-uri` | (see below) | Full PostgreSQL URI. Overridden when `POSTGRES_PASSWORD` is set. |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB` | postgres, -, localhost, 5432, fdk_harvest_admin | Used to build URI with encoded password when `POSTGRES_PASSWORD` is set. |
| `MONGO_DATABASE` / `--mongo-database` | `fdkHarvestAdmin` | MongoDB database name. |

The script uses the database name from `--mongo-database` when reading collections.

## Usage

From the repo root or from `scripts/migrate-mongo-to-postgres`, use the wrapper script (creates/activates venv and installs deps if needed):

```bash
./scripts/migrate-mongo-to-postgres/run-migrate.sh --dry-run
./scripts/migrate-mongo-to-postgres/run-migrate.sh

# With env or flags
export MONGO_URI="mongodb://root:admin@localhost:27017/?authSource=admin"
export POSTGRES_URI="postgresql://user:pass@localhost:5432/fdk_harvest_admin"
./scripts/migrate-mongo-to-postgres/run-migrate.sh

./scripts/migrate-mongo-to-postgres/run-migrate.sh --skip-reports
```

Or run `migrate.py` directly after activating the venv:

```bash
cd scripts/migrate-mongo-to-postgres
source .venv/bin/activate
python migrate.py --dry-run
python migrate.py
```

## Behaviour

- **data_sources**: Inserts or updates by `id` (`ON CONFLICT (id) DO UPDATE`). `authHeader` from MongoDB is not migrated (not present in the new schema).
- **reports**: Each document has `id` (data source id) and `reports` (map of data type → last run info). For each entry we insert one **harvest_runs** row with a new `run_id` (UUID), and map `startTime`/`endTime` to `run_started_at`/`run_ended_at`, `harvestError` → `status` FAILED/COMPLETED, `errorMessage` → `error_message`, and `changedResources`/`changedCatalogs` length → `changed_resources_count`.
- Invalid or missing required fields (e.g. unknown `dataSourceType`/`dataType`, missing `url`) are skipped and reported.

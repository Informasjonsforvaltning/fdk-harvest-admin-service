#!/usr/bin/env python3
"""
Migrate MongoDB documents from fdk-harvest-admin to PostgreSQL (fdk-harvest-admin-service).

Designed for local runs with port-forwards. Configure via environment or CLI:

  MONGO_URI=mongodb://root:admin@localhost:27017/fdkHarvestAdmin?authSource=admin
  POSTGRES_URI=postgresql://user:pass@localhost:5432/fdk_harvest_admin

  python migrate.py [--mongo-uri URI] [--postgres-uri URI] [--dry-run] [--skip-reports]
"""

import argparse
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from urllib.parse import quote_plus

# Optional deps - fail with clear message if missing
try:
    import psycopg
except ImportError:
    print("Missing dependency: pip install 'psycopg[binary]'", file=sys.stderr)
    sys.exit(1)
try:
    from pymongo import MongoClient
except ImportError:
    print("Missing dependency: pip install pymongo", file=sys.stderr)
    sys.exit(1)


# MongoDB collection names (from fdk-harvest-admin env)
MONGO_DATABASE = "fdkHarvestAdmin"
DATA_SOURCES_COLLECTION = "dataSources"
REPORTS_COLLECTION = "reports"

# Allowed values (PostgreSQL CHECK constraints)
DATA_SOURCE_TYPES = {"SKOS-AP-NO", "DCAT-AP-NO", "CPSV-AP-NO", "TBX", "ModellDCAT-AP-NO"}
DATA_TYPES = {"concept", "dataset", "informationmodel", "dataservice", "publicService", "event"}


def parse_ts(s: str | None):
    """Parse ISO-ish timestamp string to datetime; return None if missing/invalid."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    # Try common formats
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.strptime(s.replace("Z", "+00:00"), fmt.replace("Z", "%z"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            continue
    return None


def truncate(s: str | None, max_len: int) -> str | None:
    if s is None:
        return None
    s = str(s).strip()
    return s[:max_len] if len(s) > max_len else (s or None)


def migrate_data_sources(mongo_client, pg_conn, dry_run: bool, mongo_database: str):
    """Copy dataSources -> data_sources."""
    db = mongo_client[mongo_database]
    coll = db[DATA_SOURCES_COLLECTION]
    cursor = coll.find({})
    rows = []
    skipped = 0
    for doc in cursor:
        ds_type = (doc.get("dataSourceType") or "").strip()
        dtype = (doc.get("dataType") or "").strip()
        if ds_type not in DATA_SOURCE_TYPES:
            print(f"  [skip] data_sources id={doc.get('id')}: invalid dataSourceType '{ds_type}'")
            skipped += 1
            continue
        if dtype not in DATA_TYPES:
            print(f"  [skip] data_sources id={doc.get('id')}: invalid dataType '{dtype}'")
            skipped += 1
            continue
        url = (doc.get("url") or "").strip()
        if not url:
            print(f"  [skip] data_sources id={doc.get('id')}: missing url")
            skipped += 1
            continue
        row = (
            doc.get("id"),
            ds_type,
            dtype,
            truncate(url, 2048) or "",
            truncate(doc.get("acceptHeaderValue"), 255),
            (doc.get("publisherId") or "").strip() or None,
            doc.get("description"),
        )
        if not row[0]:
            print("  [skip] data_sources: missing id")
            skipped += 1
            continue
        rows.append(row)

    if dry_run:
        print(f"  [dry-run] Would insert {len(rows)} data_sources (skipped {skipped})")
        return len(rows), skipped

    if not rows:
        print("  No data_sources to insert.")
        return 0, skipped

    with pg_conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO data_sources (id, data_source_type, data_type, url, accept_header, publisher_id, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                data_source_type = EXCLUDED.data_source_type,
                data_type = EXCLUDED.data_type,
                url = EXCLUDED.url,
                accept_header = EXCLUDED.accept_header,
                publisher_id = EXCLUDED.publisher_id,
                description = EXCLUDED.description
            """,
            rows,
        )
    pg_conn.commit()
    print(f"  Inserted/updated {len(rows)} data_sources (skipped {skipped})")
    return len(rows), skipped


def migrate_reports(mongo_client, pg_conn, dry_run: bool, mongo_database: str):
    """Copy reports -> harvest_runs (one row per data source + data type)."""
    db = mongo_client[mongo_database]
    coll = db[REPORTS_COLLECTION]
    cursor = coll.find({})
    rows = []
    skipped = 0
    for doc in cursor:
        data_source_id = (doc.get("id") or "").strip()
        if not data_source_id:
            print("  [skip] reports: document missing id")
            skipped += 1
            continue
        reports_map = doc.get("reports") or {}
        if not isinstance(reports_map, dict):
            print(f"  [skip] reports id={data_source_id}: reports not a map")
            skipped += 1
            continue
        for data_type, report in reports_map.items():
            if not isinstance(report, dict):
                continue
            dtype = (report.get("dataType") or data_type or "").strip()
            if dtype not in DATA_TYPES:
                dtype = (data_type or "").strip() if data_type in DATA_TYPES else "dataset"
            run_id = str(uuid.uuid4())
            start_time = report.get("startTime")
            end_time = report.get("endTime")
            run_started_at = parse_ts(start_time)
            run_ended_at = parse_ts(end_time)
            if not run_started_at:
                run_started_at = run_ended_at or datetime.now(timezone.utc)
            status = "FAILED" if report.get("harvestError") else "COMPLETED"
            error_message = report.get("errorMessage")
            if error_message is not None and isinstance(error_message, str):
                error_message = error_message.strip() or None
            changed = report.get("changedResources") or report.get("changedCatalogs")
            changed_count = len(changed) if isinstance(changed, list) else None
            now = datetime.now(timezone.utc)
            row = (
                run_id,
                data_source_id,
                dtype,
                run_started_at,
                run_ended_at,
                status,
                error_message,
                changed_count,
                now,
                now,
            )
            rows.append(row)

    if dry_run:
        print(f"  [dry-run] Would insert {len(rows)} harvest_runs (skipped {skipped})")
        return len(rows), skipped

    if not rows:
        print("  No harvest_runs to insert from reports.")
        return 0, skipped

    with pg_conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO harvest_runs (
                run_id, data_source_id, data_type,
                run_started_at, run_ended_at, status, error_message,
                changed_resources_count, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO NOTHING
            """,
            rows,
        )
    pg_conn.commit()
    print(f"  Inserted {len(rows)} harvest_runs from reports (skipped {skipped})")
    return len(rows), skipped


def main():
    ap = argparse.ArgumentParser(
        description="Migrate MongoDB (fdk-harvest-admin) to PostgreSQL (fdk-harvest-admin-service)."
    )
    ap.add_argument(
        "--mongo-uri",
        default=os.environ.get("MONGO_URI", "mongodb://root:admin@localhost:27017/?authSource=admin"),
        help="MongoDB connection URI (default: MONGO_URI or localhost with authSource=admin)",
    )
    ap.add_argument(
        "--postgres-uri",
        default=os.environ.get(
            "POSTGRES_URI",
            "postgresql://postgres:postgres@localhost:5432/fdk_harvest_admin",
        ),
        help="PostgreSQL connection URI (default: POSTGRES_URI or localhost)",
    )
    ap.add_argument(
        "--mongo-database",
        default=os.environ.get("MONGO_DATABASE", MONGO_DATABASE),
        help="MongoDB database name",
    )
    ap.add_argument("--dry-run", action="store_true", help="Do not write to PostgreSQL")
    ap.add_argument("--skip-reports", action="store_true", help="Only migrate data_sources, not reports -> harvest_runs")
    args = ap.parse_args()

    # Build URIs from component env vars with URL-encoded passwords (so .env can use raw passwords)
    mongo_uri = args.mongo_uri
    if os.environ.get("MONGO_PASSWORD") is not None:
        user = os.environ.get("MONGO_USER", "root")
        password = os.environ.get("MONGO_PASSWORD", "")
        host = os.environ.get("MONGO_HOST", "localhost:27017")
        db = os.environ.get("MONGO_DATABASE", MONGO_DATABASE)
        auth_source = os.environ.get("MONGO_AUTH_SOURCE", "admin")
        # directConnection=true: use only this host (required when port-forwarding to a replica set member)
        mongo_uri = f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}/{db}?authSource={quote_plus(auth_source)}&directConnection=true"
    postgres_uri = args.postgres_uri
    if os.environ.get("POSTGRES_PASSWORD") is not None:
        user = os.environ.get("POSTGRES_USER", "postgres")
        password = os.environ.get("POSTGRES_PASSWORD", "")
        host = os.environ.get("POSTGRES_HOST", "localhost")
        port = os.environ.get("POSTGRES_PORT", "5432")
        db = os.environ.get("POSTGRES_DB", "fdk_harvest_admin")
        postgres_uri = f"postgresql://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{quote_plus(db)}"

    print("MongoDB URI:", re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", mongo_uri))
    print("PostgreSQL URI:", re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", postgres_uri))
    if args.dry_run:
        print("DRY RUN – no writes to PostgreSQL")
    print()

    mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        mongo_client.admin.command("ping")
    except Exception as e:
        print("MongoDB connection failed:", e)
        sys.exit(1)

    try:
        with psycopg.connect(postgres_uri, autocommit=False) as pg_conn:
            print("1. Migrating data_sources (dataSources -> data_sources)...")
            migrate_data_sources(mongo_client, pg_conn, args.dry_run, args.mongo_database)
            if not args.skip_reports:
                print("2. Migrating reports -> harvest_runs...")
                migrate_reports(mongo_client, pg_conn, args.dry_run, args.mongo_database)
            else:
                print("2. Skipping reports (--skip-reports).")
    except Exception as e:
        print("PostgreSQL error:", e)
        sys.exit(1)
    finally:
        mongo_client.close()

    print("\nDone.")


if __name__ == "__main__":
    main()

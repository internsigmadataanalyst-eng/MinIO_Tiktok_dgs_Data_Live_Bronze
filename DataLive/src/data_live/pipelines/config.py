# src/data_live/pipelines/config.py
"""Shared constants and mutable pipeline state for the Data Live ETL."""

import os

from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "database-sigma"
WATERMARK_PATH = "watermarks/data_live_toko.json"

SRC_GSHEET = {"system": "Google_Sheets", "entity": "Data Live"}
SRC_MINIO = {"system": "MinIO", "entity": WATERMARK_PATH}
TGT_MINIO = {"system": "MinIO", "entity": "data/live"}
TGT_MINIO_QUARANTINE = {"system": "MinIO", "entity": "quarantine/data_live/"}
TGT_BQ_BRONZE = {"system": "BigQuery", "entity": f"{PROJECT_ID}.BRONZE_DB.bronze_live"}
TGT_BQ_SILVER = {"system": "BigQuery", "entity": f"{PROJECT_ID}.SILVER_DB.silver_tt_live"}

WHITELIST_SHEETS = set(
    s.strip() for s in os.getenv("WHITELIST_SHEETS", "ian").split(",") if s.strip()
)

BQ_TARGETS = [
    {"table": TGT_BQ_BRONZE["entity"], "action": "append"},
    {"table": TGT_BQ_SILVER["entity"], "action": "MERGE (silver upsert)"},
]

_failure_ctx = {
    "stage": "",
    "minio_files": [],
    "rollback_hint": "",
}


def bq_full_load(rows: int) -> list[dict]:
    """BQ-update summary for a run that appended rows to bronze + merged silver."""
    return [
        {"table": TGT_BQ_BRONZE["entity"], "action": "append", "rows": rows},
        {"table": TGT_BQ_SILVER["entity"], "action": "MERGE (silver upsert)"},
    ]


def bq_noop() -> list[dict]:
    """BQ-update summary for a successful run with no new rows to load."""
    return [
        {"table": TGT_BQ_BRONZE["entity"], "action": "no change", "rows": 0},
        {"table": TGT_BQ_SILVER["entity"], "action": "no change"},
    ]


def get_credentials():
    from google.oauth2 import service_account

    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")
    return service_account.Credentials.from_service_account_file(sa_path)

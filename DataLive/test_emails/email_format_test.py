# DataLive/test_emails/email_format_test.py
"""Email-format test harness for the Data Live ETL.

Runs the REAL `run_daily_etl` code path against DUMMY data while every
MinIO / BigQuery / Silver infractructure write is replaced with an in-memory
no-op, so nothing touches production storage. The only real side effect is the
SMTP channel: each pipeline action that would fire an alert sends a genuine
email to NOTIFY_RECIPIENTS.

Scenarios cover every builder in utils/notify.py:
  1  happy full load           -> Quarantine + Recovery + Success (silver done)
  2a all rows up-to-date       -> Success ("data is up to date - no new rows")
  2b rows deduped by idem. gate-> Success ("no new rows - watermark advanced")
  3  pre-flight access error   -> Gate-1 abort email
  4  one sheet caught up       -> Gate-2 abort email (with drift table)
  5  BigQuery load raises      -> Pipeline FAILED email

Conventions: run from DataLive/ with
  ..\\.venv\\Scripts\\python.exe test_emails\\email_format_test.py
Add --self-check to only print SMTP config without sending anything.
"""
import argparse
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # DataLive/
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

# ---- force the real email backend BEFORE notify.py reads its config ----
os.environ["NOTIFY_ENABLED"] = "true"
os.environ["NOTIFY_BACKEND"] = "smtp"
os.environ["NOTIFY_AI_EXPLAIN"] = "true"
_gemini_key_file = Path("C:/CREDS/bg-test/gemini-api-key.txt")
if _gemini_key_file.exists() and not os.getenv("GEMINI_API_KEY"):
    os.environ["GEMINI_API_KEY"] = str(_gemini_key_file)

import pandas as pd

import src.data_live.utils.notify as notify

_sent: list[tuple[str, bool]] = []
_orig_send = notify.send_alert_email


def _wrapped_send(subject, body_html, recipients=None, dry_run=False):
    """Record every send call, then hand over to the real SMTP implementation."""
    _sent.append((subject, dry_run))
    return _orig_send(subject, body_html, recipients=recipients, dry_run=dry_run)


notify.send_alert_email = _wrapped_send

# Imported AFTER the notify wrapper so run_daily_etl binds it too.
from src.data_live.pipelines import run_daily_etl as rde
from src.data_live.pipelines.config import _failure_ctx, BQ_TARGETS
from src.data_live.utils.transform_utils import (
    NUMERIC_COLS,
    PERCENT_COLS,
    validate_and_normalize_raw,
)
from src.data_live.transform.clean_bronze import build_bronze_live

# ---------------------------------------------------------------------------
# Dummy data builders
# ---------------------------------------------------------------------------

TODAY = date.today()
WM_DATE = TODAY - timedelta(days=4)  # watermark sits 4 days in the past

OTHER_HDRS = [
    "Tanggal", "Toko", "ID Kreator", "Kreator", "Nama panggilan",
    "Waktu Live", "Durasi",
]
HDRS = OTHER_HDRS + NUMERIC_COLS + PERCENT_COLS

SHEETS = [
    ("matz", "SH_KEY_MATZ"),
    ("ian", "SH_KEY_IAN"),
    ("deni", "SH_KEY_DENI"),
    ("riwa", "SH_KEY_RIWA"),
    ("imam", "SH_KEY_IMAM"),
]


def _fmt(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def row(tanggal: str, toko: str = "TokoA", id_kreator: str = "U123") -> dict:
    r = {h: "1" for h in NUMERIC_COLS}
    r.update({h: "0" for h in PERCENT_COLS})
    r["Tanggal"] = tanggal
    r["Toko"] = toko
    r["ID Kreator"] = id_kreator
    r["Kreator"] = "Kreator X"
    r["Nama panggilan"] = "X"
    r["Waktu Live"] = f"{tanggal}/20:15:00"
    r["Durasi"] = "01:30:00"
    return r


def build_raw(dates: list[str], *, bads: bool = True) -> pd.DataFrame:
    """Constructs the raw concat frame exactly like fetch_tiktok_data_live:
    rows tagged with `creds` + `sheet_name`, one frame per registered sheet."""
    frames = []
    for i, (sheet, env_key) in enumerate(SHEETS):
        # ID Kreator must differ per sheet: row_hash_raw uses
        # waktu_live/toko/id_kreator, so identical rows across sheets would
        # collapse in the pending drop_duplicates(subset=['row_hash_raw']).
        rows = [row(d, id_kreator=f"U{i}") for d in dates]
        if bads and sheet == "matz":
            rows.append(row("not-a-date"))                        # date_unparsable
            bad_num = row(dates[0]); bad_num["Penonton"] = "abc"  # numeric_mixed
            rows.append(bad_num)
            bad_toko = row(dates[0]); bad_toko["Toko"] = "   "    # toko_blank
            rows.append(bad_toko)
            rows.append(row(_fmt(TODAY + timedelta(days=3))))     # date_future
        df = pd.DataFrame(rows, columns=HDRS)
        df["creds"] = os.getenv(env_key)
        df["sheet_name"] = sheet
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def watermark(sheets=SHEETS, wm_date: date = WM_DATE):
    records = []
    for sheet, env_key in sheets:
        creds = os.getenv(env_key)
        records.append({
            "creds": creds,
            "sheet_name": sheet,
            "toko": "TokoA",
            "last_processed_date": wm_date.isoformat(),
            "updated_at": datetime.now().isoformat(),
        })
    wmap = {
        (r["creds"], r["sheet_name"], r["toko"]): r["last_processed_date"][:10]
        for r in records
    }
    return wmap, records


def status_frame(behind: dict, error_sheet: str | None = None) -> pd.DataFrame:
    """Pre-flight gate status DataFrame — sheet_name/grain/status columns."""
    rows = []
    for sheet, _ in SHEETS:
        has_error = error_sheet == sheet
        rows.append({
            "sheet_name": sheet,
            "grain": "TokoA",
            "logical_sheet": "Performa Sesi Live",
            "sheet_max_tanggal": pd.Timestamp(TODAY - timedelta(days=1)),
            "last_processed_date": pd.Timestamp(WM_DATE),
            "is_behind": behind.get(sheet, True) and not has_error,
            "status": f"error: Failed to open worksheet {sheet}" if has_error else "ok",
        })
    return pd.DataFrame(rows)


def compute_row_hashes(df_raw: pd.DataFrame, wmap: dict) -> set:
    """Exact row_hash_raw set the idempotency gate would see in bronze (in-memory)."""
    df_valid, _, _ = validate_and_normalize_raw(df_raw, NUMERIC_COLS, percent_cols=PERCENT_COLS)
    df_b, _ = build_bronze_live(df_valid, sheet_watermarks=wmap)
    return set(df_b["row_hash_raw"].astype(str))


# ---------------------------------------------------------------------------
# In-memory write blocking (the "comment out production paths" equivalent)
# ---------------------------------------------------------------------------

class MiniFake:
    """Stand-in for the MinIO client: every put is recorded, never transmitted."""

    def __init__(self):
        self.writes: list[tuple] = []

    def put_object(self, bucket, key, data, length=0, **kw):
        self.writes.append((bucket, key, length))
        print(f"[BLOCKED] MinIO put_object {bucket}/{key} ({length} bytes) — in-memory")

    def stat_object(self, bucket, key):
        raise FileNotFoundError("BLOCKED in-memory MinIO read")

    def get_object(self, bucket, key):
        raise FileNotFoundError("BLOCKED in-memory MinIO read")


_fake = MiniFake()


def _blocked_bq_append(*a, **k):
    df = k.get("df", a[0] if a else None)
    table = k.get("table_id", a[1] if len(a) > 1 else "?")
    mode = k.get("if_exists", a[3] if len(a) > 3 else "?")
    print(f"[BLOCKED] BigQuery append in-memory: table={table} if_exists={mode} rows={len(df) if df is not None else 0}")


def _blocked_silver(*a, **k):
    print("[BLOCKED] Silver MERGE in-memory (no write)")


def _blocked_watermark(*a, **k):
    max_dates = a[4] if len(a) > 4 else k.get("sheet_max_dates")
    keys = ", ".join(f"{k2}" for k2 in max_dates or {})
    print(f"[BLOCKED] watermark update in-memory, {len(max_dates or {})} grain(s): {keys}")


def _blocked_quarantine(*a, **k):
    print("[BLOCKED] quarantine parquet write in-memory")


def configure(df_raw, sframe, wmap, wrecords, *, resolved=(), existing_hashes=None, fail_load=False):
    """Point every infra/write seam in run_daily_etl at an in-memory stand-in."""
    rde.get_gspread_client = lambda: None
    rde.init_spreadsheet_objects = lambda gc: {}
    rde.get_minio_client = lambda: (_fake, "test-email-format")
    rde.get_sheet_watermarks = lambda *a, **k: (wmap, wrecords)
    rde.data_live_watermark_check = lambda *a, **k: sframe
    rde.fetch_tiktok_data_live = lambda *a, **k: df_raw
    rde.filter_already_quarantined = lambda *a, **k: a[2]
    rde.sync_error_manifest = lambda *a, **k: list(resolved)
    rde.update_sheet_watermarks = _blocked_watermark
    rde.write_quarantine = _blocked_quarantine
    rde._fetch_existing_bronze_hashes = lambda *a, **k: set(existing_hashes or [])

    def _boom(*a, **k):
        raise RuntimeError("SIMULATED BigQuery bronze append failure (test harness)")

    rde.load_df = _boom if fail_load else _blocked_bq_append
    rde.merge_to_silver = _blocked_silver


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def scenario_1_happy_full_load():
    """Quarantine + Recovery + Success("full load finished — Silver MERGE done")."""
    wmap, wrecs = watermark()
    dates = [_fmt(TODAY - timedelta(days=k)) for k in (3, 2, 1)]
    df_raw = build_raw(dates, bads=True)
    # error_date in the manifest must be ISO — the recovery matcher compares
    # against df_valid["Tanggal"].dt.date().astype(str) (ISO).
    resolved = [{
        "sheet_name": "matz",
        "creds": os.getenv("SH_KEY_MATZ"),
        "toko": "TokoA",
        "error_date": (TODAY - timedelta(days=2)).isoformat(),
        "n_rows": 1,
        "path": "quarantine/date=99999999/quarantine_999999999999.parquet",
        "status": "fixed",
    }]
    configure(df_raw, status_frame({}), wmap, wrecs, resolved=resolved)
    rde.run_daily_etl(dry_run=False)


def scenario_2a_up_to_date():
    """Success("data is up to date — no new rows") — no quarantine, no recovery."""
    df_raw = build_raw([_fmt(WM_DATE)], bads=False)
    wmap, wrecs = watermark()
    configure(df_raw, status_frame({}), wmap, wrecs)
    rde.run_daily_etl(dry_run=False)


def scenario_2b_watermark_advance():
    """Success("no new rows — watermark advanced"): gate passed, rows behind,
    but the idempotency gate sees them as already present in bronze."""
    wmap, wrecs = watermark()
    dates = [_fmt(TODAY - timedelta(days=k)) for k in (3, 2, 1)]
    df_raw = build_raw(dates, bads=False)
    hashes = compute_row_hashes(df_raw, wmap)
    configure(df_raw, status_frame({}), wmap, wrecs, existing_hashes=hashes)
    rde.run_daily_etl(dry_run=False)


def scenario_3_gate1_access_error():
    """Gate-1 abort email (single email, run stops)."""
    df_raw = build_raw([_fmt(WM_DATE)], bads=False)
    wmap, wrecs = watermark()
    configure(df_raw, status_frame({}, error_sheet="matz"), wmap, wrecs)
    rde.run_daily_etl(dry_run=False)


def scenario_4_gate2_caught_up():
    """Gate-2 abort email incl. the per-sheet watermark drift table."""
    df_raw = build_raw([_fmt(WM_DATE)], bads=False)
    wmap, wrecs = watermark()
    behind = {"matz": True, "deni": False, "riwa": True, "imam": True, "ian": False}
    configure(df_raw, status_frame(behind), wmap, wrecs)
    rde.run_daily_etl(dry_run=False)


def scenario_5_pipeline_failure():
    """Pipeline FAILED email via the main.py failure path (run stops)."""
    wmap, wrecs = watermark()
    dates = [_fmt(TODAY - timedelta(days=k)) for k in (3, 2, 1)]
    df_raw = build_raw(dates, bads=False)
    configure(df_raw, status_frame({}), wmap, wrecs, fail_load=True)
    try:
        rde.run_daily_etl(dry_run=False)
    except Exception as e:  # mirror main.py's except block
        print(f">>> [DRIVER] caught pipeline failure: {type(e).__name__}: {e}")
        subject, body_html = notify.build_pipeline_failure_email(
            e,
            run_key=datetime.now().strftime("%Y%m%d%H%M"),
            log_path="",
            stage=_failure_ctx.get("stage", ""),
            bq_updates=BQ_TARGETS,
            minio_files=_failure_ctx.get("minio_files") or [],
            rollback_hint=_failure_ctx.get("rollback_hint", ""),
            enable_explanation=True,
        )
        notify.send_alert_email(subject, body_html, dry_run=False)


SCENARIOS = {
    "1": ("happy full load (quarantine + recovery + success)", scenario_1_happy_full_load),
    "2a": ("up-to-date -> success(no new rows)", scenario_2a_up_to_date),
    "2b": ("idempotency-deduped -> success(watermark advanced)", scenario_2b_watermark_advance),
    "3": ("gate-1 access error abort", scenario_3_gate1_access_error),
    "4": ("gate-2 caught-up abort", scenario_4_gate2_caught_up),
    "5": ("BigQuery load failure", scenario_5_pipeline_failure),
}


def _matches(scenario: str, select: list[str]) -> bool:
    if not select:
        return True
    if scenario in select:
        return True
    return not any(s in select for s in SCENARIOS) and scenario == select[0]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--self-check", action="store_true",
                    help="print notify SMTP config and exit without sending anything")
    ap.add_argument("--scenario", nargs="*",
                    help="run only these scenarios: 1 2a 2b 3 4 5 (default: all)")
    args = ap.parse_args(argv)

    print("=" * 70)
    print("EMAIL-FORMAT TEST — Data Live ETL (dummy data, writes blocked in-memory)")
    print("=" * 70)
    notify.notify_self_check()
    if args.self_check:
        print("\nConfig OK. Nothing sent (--self-check).")
        return 0

    to_run = args.scenario or sorted(SCENARIOS, key=lambda s: (len(s), s))
    for name in to_run:
        label, fn = SCENARIOS[name]
        print("\n" + "=" * 70)
        print(f"SCENARIO {name}: {label}")
        print("=" * 70)
        fn()

    print("\n" + "=" * 70)
    print("SENT EMAIL SUMMARY (real SMTP attempts)")
    print("=" * 70)
    for i, (subject, dry) in enumerate(_sent, 1):
        tag = "DRY-SKIP" if dry else "SENT"
        print(f"  {i:>2}. [{tag}] {subject}")
    print(f"  Total send_alert_email calls recorded: {len(_sent)}")
    print(f"  Blocked MinIO puts during the run(s): {len(_fake.writes)}")
    notify.close_smtp()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
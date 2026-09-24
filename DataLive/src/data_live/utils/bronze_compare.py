# src/data_live/utils/bronze_compare.py
"""Show the current MinIO watermark.

Ported from Pesanan_Affiliasi and parameterized per project so it works for the
sibling ETLs. Delivered as a copied module per repository.

Highlights:
- `effective_watermark_changes` : only reports watermark updates whose candidate
  max-date actually differs from the stored value (avoids fake 'watermark update'
  lines in dry-run when nothing changes).
- `show_watermark` : shows only the MinIO watermark (no bronze read/compare).
"""
import pandas as pd

PROJECT_ID = "database-sigma"


def effective_watermark_changes(
    watermark_records: list, sheet_max_dates: dict
) -> dict:
    """Returns only the (creds, sheet_name, toko) -> max_date entries whose
    candidate max_date actually DIFFERS from the currently-stored watermark.

    Groups with no stored watermark yet (new groups) are included. This avoids
    reporting a 'watermark update' when nothing would change.
    """
    stored = {}
    for rec in watermark_records or []:
        creds = str(rec.get("creds") or "")
        sheet_name = str(rec.get("sheet_name") or "")
        toko = str(rec.get("toko") or "")
        date_val = str(rec.get("last_processed_date") or rec.get("last_update") or "").strip()[:10]
        stored[(creds, sheet_name, toko)] = date_val

    changes = {}
    for (creds, sheet_name, toko), max_date in sheet_max_dates.items():
        key = (str(creds), str(sheet_name or ""), str(toko or ""))
        if max(stored.get(key, ""), "").strip() == str(max_date).strip()[:10]:
            continue
        changes[key] = str(max_date)
    return changes


def _show_watermark(watermark_records: list):
    if not watermark_records:
        print("[WATERMARK] (tidak ada watermark MinIO yang ditemukan)")
        return
    print("\n[WATERMARK] Current watermark (MinIO):")
    wm_cols = [c for c in ("creds", "sheet_name", "toko", "last_processed_date")
               if c in (watermark_records[0] or {})]
    wm_df = pd.DataFrame(watermark_records)[wm_cols].sort_values(wm_cols)
    print(wm_df.to_string(index=False))


def show_watermark(watermark_records: list):
    """Shows only the MinIO watermark (no bronze read/compare)."""
    _show_watermark(watermark_records)


def fmt_drift_date(val) -> str:
    """Format a drift-check date value for the gate-2 email table ('' for NaT)."""
    try:
        if val is None or pd.isna(val):
            return ""
        if hasattr(val, "strftime"):
            return val.strftime("%Y-%m-%d")
        return str(val)
    except Exception:
        return str(val or "")


def build_drift_rows(status_df: pd.DataFrame) -> list[dict]:
    """Per-sheet watermark drift summary from the pre-flight check.

    Column order matches the gate-2 email table: Sheet | Toko | Sheet max date |
    Current watermark | Status (BEHIND/ok). Sorted by sheet_name then toko.
    """
    rows = []
    for _, row in status_df.iterrows():
        rows.append({
            "sheet_name": str(row.get("sheet_name") or ""),
            "toko": str(row.get("grain") or ""),
            "gsheet_max": fmt_drift_date(row.get("sheet_max_tanggal")),
            "watermark": fmt_drift_date(row.get("last_processed_date")),
            "status": "BEHIND" if row.get("is_behind") else "ok",
        })
    rows.sort(key=lambda r: (r["sheet_name"], r["toko"]))
    return rows


def write_wm_log(log_folder, run_key, status_df, sheet_passes, verdict_msg):
    """Write the watermark drift check log file."""
    from datetime import datetime
    from src.data_live.utils.log import write_section_log

    wm_log_lines = []
    wm_log_lines.append(
        f"=== WATERMARK DRIFT CHECK — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n"
    )

    wm_log_lines.append("-" * 50)
    wm_log_lines.append("DATASET: DATA LIVE (toko grain)")
    wm_log_lines.append("-" * 50)
    wm_log_lines.append(f"  {'sheet':<10} {'grain':<12} {'gsheet':<12} {'wm':<12} {'status'}")
    for _, row in status_df.iterrows():
        wm_log_lines.append(
            f"  {row['sheet_name']:<10} {str(row['grain']):<12} "
            f"{str(row['sheet_max_tanggal']):<12} {str(row['last_processed_date']):<12} "
            f"{'BEHIND' if row['is_behind'] else 'ok'}"
        )

    wm_log_lines.append(f"\nGate verdict: {verdict_msg}")
    pass_count = int(sheet_passes.sum()) if len(sheet_passes) else 0
    total = len(sheet_passes)
    wm_log_lines.append(f"  {pass_count}/{total} sheets have >=1 toko behind")

    write_section_log(log_folder, f"wm_monitor_logs_{run_key}.log", "\n".join(wm_log_lines) + "\n")
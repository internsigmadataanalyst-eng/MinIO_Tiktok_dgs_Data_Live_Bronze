# src/data_live/ingestion/fetch_data_live_gsheet.py
import os
import pandas as pd
import gspread

from src.data_live.utils.gsheet_client import with_retry_on_429


SHEET_REGISTRY = {
    "matz": "SH_KEY_MATZ",
    "ian": "SH_KEY_IAN",
    "deni": "SH_KEY_DENI",
    "riwa": "SH_KEY_RIWA",
    "imam": "SH_KEY_IMAM",
}


def fetch_tiktok_data_live(gc: gspread.Client, spreadsheet_objects: dict | None = None) -> pd.DataFrame:
    """
    Ambil data produk dari GSheet yang terdaftar di SHEET_REGISTRY,
    tag tiap sheet dengan kolom 'sheet_name', lalu concat jadi satu
    DataFrame raw (belum dibersihkan).

    Reuses the pre-opened Spreadsheet objects (from init_spreadsheet_objects)
    when provided to avoid re-opening the same spreadsheet, falling back to a
    retried open_by_key otherwise. Every GSheet API call is wrapped in the 429
    retry helper.
    """
    spreadsheet_objects = spreadsheet_objects or {}
    frames = []
    for sheet_name, env_key in SHEET_REGISTRY.items():
        sh = spreadsheet_objects.get(env_key)
        if sh is None:
            sh = with_retry_on_429(gc.open_by_key, os.getenv(env_key))
        ws = with_retry_on_429(sh.worksheet, "Performa Sesi Live")
        values = with_retry_on_429(ws.get_all_values)
        df_sheet = pd.DataFrame(values[3:], columns=values[2])
        df_sheet = df_sheet.loc[:, ~df_sheet.columns.duplicated()]
        df_sheet["creds"] = os.getenv(env_key)
        df_sheet["sheet_name"] = sheet_name
        frames.append(df_sheet)
        print(f"[INGEST] {sheet_name}: {len(df_sheet)} rows")

    tiktok_live = pd.concat(frames, ignore_index=True)
    return tiktok_live

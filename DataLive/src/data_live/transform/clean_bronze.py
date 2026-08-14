# src/data_live/transform/clean_bronze.py
import uuid
import hashlib
from datetime import datetime, timezone

import pandas as pd

from src.data_live.utils.transform_utils import (
    clean_numeric_columns,
    parse_mixed_dates,
    to_snake_case,
)
from src.data_live.utils.minio_client import filter_by_sheet_watermark


NUMERIC_COLS = [
    "Nilai barang dagangan bruto (LIVE) (Rp)",
    "Produk yang ditambahkan",
    "Produk Terjual",
    "Pesanan SKU yang dibuat",
    "Pesanan SKU (LIVE)",
    "Produk yang terjual dari LIVE",
    "Pembeli",
    "Harga Rata-Rata (Rp)",
    "GMV yang didapat dari LIVE (Rp)",
    "Penonton",
    "Live Stream Dilihat",
    "Durasi menonton rata-rata (Siaran LIVE)",
    "Komentar",
    "Live Dibagikan",
    "Suka pada LIVE",
    "Pengikut baru (Video kreator)",
    "Produk Dilihat",
    "Klik Produk",
]


def _canon(x):
    import pandas as pd

    x = "" if pd.isna(x) else str(x).strip()
    return x.upper()


def build_bronze_live(
    tiktok_live_raw: pd.DataFrame, sheet_watermarks: dict | None = None
) -> tuple[pd.DataFrame, dict]:
    """
    Dari raw GSheet → cleaning numeric + tanggal + snake_case,
    tambah snapshot_ts, snapshot_date, run_id, row_hash_raw.
    Filter incremental per sheet_name berdasarkan watermark (sheet_watermarks).
    Output: (df siap di-load ke BRONZE_DB.bronze_live, sheet_max_dates)
    """
    # numeric cleaning
    tiktok_live_clean1 = clean_numeric_columns(
        tiktok_live_raw, NUMERIC_COLS, fillna_value=0
    )

    tiktok_live_clean1["Tanggal"] = parse_mixed_dates(
        tiktok_live_clean1["Tanggal"], return_date=False
    )

    # copy & snake_case
    df = tiktok_live_clean1.copy()
    df.columns = df.columns.map(to_snake_case)

    # buang baris tanpa id
    df = df[df["waktu_live"].astype(str).str.strip() != ""]

    # tambahkan pukul live
    df['pukul_live'] = df['waktu_live'].str.split('/').str[3].str.split(':').str[0]
    df['pukul_live'] = pd.to_numeric(df['pukul_live'], errors='coerce')

    # snapshot fields
    now_utc = datetime.now(timezone.utc)
    df["snapshot_ts"] = now_utc
    df["snapshot_date"] = now_utc.date()
    df["run_id"] = str(uuid.uuid4())

    # row_hash_raw: sesuai scriptmu
    cols_for_hash = ["waktu_live","toko","id_kreator","produk_yang_ditambahkan","produk_dilihat"]

    df["row_hash_raw"] = (
        df[cols_for_hash]
        .map(_canon)
        .astype(str)
        .agg("||".join, axis=1)
        .apply(lambda s: hashlib.sha256(s.encode()).hexdigest())
    )

    df = df.loc[:, df.columns != ""]

    # Filter incremental per sheet (creds-keyed) berdasarkan watermark
    if "creds" in df.columns:
        df, sheet_max_dates = filter_by_sheet_watermark(
            df, "creds", "tanggal", sheet_watermarks or {}
        )
    else:
        sheet_max_dates = {}

    # Hapus kolom bermasalah / tidak ada dalam schema BigQuery
    # NOTE: creds & sheet_name sengaja DIPERTAHANKAN di level bronze.
    cols_to_drop = [
        "gmv_live_rp",
        "gmv_tidak_langsung_dari_live_rp",
        "produk_yang_terjual_melalui_live",
        "produk_yang_terjual_dari_live_secara_tidak_langsung",
    ]
    df = df.drop(columns=cols_to_drop, errors="ignore")


    return df, sheet_max_dates
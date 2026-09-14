# ============================================================
# PHASE 01 — SOURCE DATA FETCH
# ============================================================

# LIVE SESSION
query_live_session = """
SELECT
  id_absensi,
  business_date,
  platform,
  toko,
  akun,
  ruangan,
  sesi,
  talent
FROM `database-sigma.SILVER_DB.silver_live_session`
"""

# TIKTOK LIVE
query_live_tiktok_toko = """
SELECT
  tanggal,
  pukul_live,
  id_kreator,
  UPPER(nama_panggilan) AS nama_panggilan,
  toko,
  SAFE_CAST(gmv_bruto_live AS INT64) AS gmv_live,
  produk_ditambahkan,
  produk_terjual_dari_live,
  live_stream_dilihat,
  pembeli,
  penonton,
  produk_dilihat,
  klik_produk,
  ctr_live,
  cvr_live
FROM `database-sigma.SILVER_DB.silver_tt_live`;
"""

# SHOPEE LIVE

query_live_shopee_toko = """
SELECT
  tanggal,
  platform,
  pukul_live,
  id_kreator,
  UPPER(nama_panggilan) AS nama_panggilan,
  toko,
  SAFE_CAST(gmv_live AS INT64) AS gmv_live,
  produk_ditambahkan,
  produk_terjual_dari_live,
  live_stream_dilihat,
  pembeli,
  penonton,
  durasi_tonton_rata
FROM `database-sigma.SILVER_DB.silver_shopee_live`;
"""


# ============================================================
# PHASE 02 — COMBINE LIVE DATA
# Source:
#   - query_live_shopee_toko
#   - query_live_tiktok_toko
# ============================================================

combined_live_data = pd.concat(
    [
        query_live_shopee_toko,
        query_live_tiktok_toko,
    ],
    ignore_index=True,
)


# ============================================================
# PHASE 03 — ASSIGN SHIFT / SESI
# ============================================================

def assign_shift(df):
    """
    Menambahkan kolom 'shift' berdasarkan kolom 'pukul_live'
    (integer jam) dengan aturan seperti di formula Google Sheets.
    """

    def get_shift(hour):
        if pd.isna(hour):
            return ""
        elif 7 <= hour < 10:
            return "S1"
        elif 10 <= hour < 13:
            return "S2"
        elif 13 <= hour < 16:
            return "S3"
        elif 16 <= hour < 19:
            return "S4"
        elif 19 <= hour < 22:
            return "S5"
        elif hour >= 22 or hour == 0:
            return "S6"
        elif 1 <= hour < 7:
            return "S0"
        else:
            return " "

    df["sesi"] = df["pukul_live"].apply(get_shift)

    return df


combined_live_data = assign_shift(combined_live_data)


# ============================================================
# PHASE 04 — CREATE PERFORMANCE KEY
# Source:
#   combined_live_data
#   query_live_session
# ============================================================

combined_live_data["id_performa"] = (
    combined_live_data["tanggal"].astype(str)
    + "_"
    + combined_live_data["sesi"].astype(str)
    + "_"
    + combined_live_data["platform"].astype(str)
    + "_"
    + combined_live_data["nama_panggilan"].astype(str)
)


query_live_session["id_performa"] = (
    query_live_session["business_date"].astype(str)
    + "_"
    + query_live_session["sesi"].astype(str)
    + "_"
    + query_live_session["platform"].astype(str)
    + "_"
    + query_live_session["akun"].astype(str)
)


# ============================================================
# PHASE 05 — CLEAN LIVE SESSION DATA
# ============================================================

query_live_session = query_live_session[
    query_live_session["talent"].notna()
]

session_clean = query_live_session.drop_duplicates(
    subset="id_performa"
)

session_clean = session_clean[
    session_clean["talent"].notna()
]


# ============================================================
# PHASE 06 — AGGREGATE LIVE PERFORMANCE
# Source:
#   combined_live_data
# ============================================================

num_sum = [
    "gmv_live",
    "produk_ditambahkan",
    "produk_terjual_dari_live",
    "live_stream_dilihat",
    "pembeli",
    "penonton",
    "produk_dilihat",
    "klik_produk",
]

ratio_mean = [
    "ctr_live",
    "cvr_live",
    "durasi_tonton_rata",
]


agg_df = (
    combined_live_data
    .groupby("id_performa")
    .agg(
        {
            **{col: "sum" for col in num_sum},
            **{col: "mean" for col in ratio_mean},
            "pukul_live": "max",
            "tanggal": "max",
            "id_kreator": "first",
            "nama_panggilan": "first",
            "toko": "first",
            "platform": "first",
            "sesi": "first",
        }
    )
    .reset_index()
)


# ============================================================
# PHASE 7 — STANDARDIZE AGGREGATED COLUMN NAMES
# ============================================================

agg_df = agg_df.rename(
    columns={
        "sesi": "sesi_sctoko",
        "toko": "toko_sctoko",
        "nama_panggilan": "akun_sctoko",
    }
)


# ============================================================
# PHASE 8 — MERGE SESSION + LIVE PERFORMANCE
# Source:
#   session_clean
#   agg_df
# ============================================================

merged_df = pd.merge(
    session_clean[
        [
            "id_performa",
            "business_date",
            "platform",
            "toko",
            "akun",
            "ruangan",
            "sesi",
            "talent",
        ]
    ],
    agg_df[
        [
            "id_performa",
            "tanggal",
            "id_kreator",
            "akun_sctoko",
            "sesi_sctoko",
            "gmv_live",
            "toko_sctoko",
            "produk_ditambahkan",
            "produk_terjual_dari_live",
            "pembeli",
            "penonton",
            "live_stream_dilihat",
            "produk_dilihat",
            "klik_produk",
            "ctr_live",
            "cvr_live",
            "durasi_tonton_rata",
        ]
    ],
    on="id_performa",
    how="right",
)


# ============================================================
# PHASE 9 — HANDLE MISSING SESSION DATA
# ============================================================

# Hanya mengisi kolom bertipe object
merged_df.loc[
    :,
    merged_df.select_dtypes(include="object").columns
] = (
    merged_df
    .select_dtypes(include="object")
    .fillna("Data HM tidak ditemukan")
)


# ============================================================
# PHASE 10 — DATA QUALITY CHECK
# Check missing HM data within the last week
# ============================================================

from datetime import datetime, timedelta


# Calculate the date one week ago
now = datetime.now().date()
one_week_ago = now - timedelta(days=7)


# Ensure 'tanggal' column is in datetime format
# and handle NaT
merged_df["tanggal"] = (
    pd.to_datetime(
        merged_df["tanggal"],
        errors="coerce"
    )
    .dt.date
)


# Filter merged_df for rows within the last week
# and exclude NaT tanggal
filtered_last_week = merged_df[
    (merged_df["tanggal"].notna())
    & (merged_df["tanggal"] >= one_week_ago)
].copy()


# Identify all columns that have an object data type
object_cols = filtered_last_week.select_dtypes(
    include="object"
).columns


# Identify rows containing:
# 'Data HM tidak ditemukan'
mask_hm_missing = (
    filtered_last_week[object_cols]
    .apply(
        lambda col: col.astype(str)
        .str.contains(
            "Data HM tidak ditemukan",
            case=False,
            na=False
        )
    )
    .any(axis=1)
)


# Store the resulting DataFrame
filtered_data_hm_missing = filtered_last_week[
    mask_hm_missing
].copy()


# Display data quality summary
print(f"Number of rows in merged_df: {len(merged_df)}")

print(
    f"Number of rows after filtering for last week: "
    f"{len(filtered_last_week)}"
)

print(
    "Number of rows where 'Data HM tidak ditemukan' "
    "is present in object columns within the last week: "
    f"{len(filtered_data_hm_missing)}"
)

display(filtered_data_hm_missing.head())


# ============================================================
# PHASE 11 — PREPARE DATE / DATETIME COLUMNS
# Before loading to BigQuery
# ============================================================

# Convert pd.NaT values to Python None
# in date/datetime columns.
# This handles cases where 'NaT' might not be automatically
# mapped to NULL by pandas_gbq.

for col in ["business_date", "tanggal"]:

    if col in merged_df.columns:

        # Check if the column is datetime-like
        if pd.api.types.is_datetime64_any_dtype(
            merged_df[col]
        ):

            merged_df[col] = merged_df[col].apply(
                lambda x: x.date()
                if pd.notna(x)
                else None
            )

        # If the column is object type but contains pd.NaT
        elif pd.api.types.is_object_dtype(
            merged_df[col]
        ):

            merged_df[col] = merged_df[col].replace(
                {pd.NaT: None}
            )


# ============================================================
# PHASE 12 — LOAD TO BIGQUERY
# ============================================================

from pandas_gbq import to_gbq


to_gbq(
    merged_df,
    destination_table="GOLD_DB.fact_live_performa_daily",
    project_id="database-sigma",
    if_exists="replace",
    credentials=credentials_bq,
)

import re
from pathlib import Path
import duckdb
import pandas as pd


def _transpile_bq_to_duckdb(sql_content: str) -> str:
    """Helper untuk merubah sintaks BigQuery SQL agar kompatibel dengan DuckDB di Memory."""
    # 1. Hapus prefix 'database-sigma.' & tanda backtick (`)
    sql_executable = (
        sql_content.replace("`database-sigma.", "")
        .replace("database-sigma.", "")
        .replace("`", "")
    )

    # 2. Sisipkan kata 'INTO' pada klausa MERGE
    sql_executable = re.sub(
        r"\bMERGE\s+(?!INTO\b)", "MERGE INTO ", sql_executable, flags=re.IGNORECASE
    )

    # 3. Ubah EXCEPT(...) BigQuery menjadi EXCLUDE(...) DuckDB
    sql_executable = re.sub(
        r"\bEXCEPT\s*\(", "EXCLUDE (", sql_executable, flags=re.IGNORECASE
    )

    # 4. Ubah BigQuery raw string r'...' menjadi standard string '...' DuckDB
    sql_executable = re.sub(r"\br(['\"][^'\"]*['\"])", r"\1", sql_executable)

    # 5. Ubah SAFE.PARSE_TIME menjadi STRPTIME
    sql_executable = re.sub(
        r"SAFE\.PARSE_TIME\(\s*'%H:%M'\s*,\s*([^)]+)\)",
        r"TRY_CAST(STRPTIME(\1, '%H:%M') AS TIME)",
        sql_executable,
        flags=re.IGNORECASE,
    )

    # 6. Ubah konstruktor TIME(...) menjadi MAKE_TIME(...)
    sql_executable = re.sub(
        r"\bTIME\s*\(", "MAKE_TIME(", sql_executable, flags=re.IGNORECASE
    )

    # 7. Ubah fungsi & tipe data khas BigQuery
    sql_executable = re.sub(
        r"\bSAFE_CAST\b", "TRY_CAST", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"\bINT64\b", "BIGINT", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"\bFLOAT64\b", "DOUBLE", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"\bNUMERIC\b", "DECIMAL", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"FORMAT_DATE\(\s*'%F'\s*,\s*([^)]+)\)",
        r"STRFTIME(\1, '%Y-%m-%d')",
        sql_executable,
        flags=re.IGNORECASE,
    )

    return sql_executable


def test_merge_to_silver_duckdb(df_bronze: pd.DataFrame):
    """Menjalankan simulasi MERGE Bronze -> Silver di DuckDB In-Memory."""
    # 1. Init Connection
    con = duckdb.connect(":memory:")
    con.sql("INSTALL bigquery FROM community; LOAD bigquery;")

    # 2. Setup Bronze
    con.sql("CREATE SCHEMA IF NOT EXISTS BRONZE_DB;")
    con.sql("CREATE TABLE BRONZE_DB.bronze_live AS SELECT * FROM df_bronze")
    print("[BRONZE] Load to BRONZE_DB.bronze_live DONE")
    print("Show Sampel Data bronze_live:")
    con.sql("SELECT * FROM BRONZE_DB.bronze_live LIMIT 3").show()

    # 3. Setup Silver Schema & Macro
    con.sql("CREATE SCHEMA IF NOT EXISTS SILVER_DB;")
    con.sql("""
        CREATE TABLE SILVER_DB.silver_tt_live (
            tanggal DATE, toko VARCHAR, id_kreator VARCHAR, kreator VARCHAR, nama_panggilan VARCHAR,
            pukul_live INT, waktu_live_time TIME, sesi_start TIMESTAMP, gmv_bruto_live NUMERIC,
            gmv_live NUMERIC, produk_ditambahkan INT, produk_terjual INT, pesanan_sku_dibuat INT,
            pesanan_sku_live INT, produk_terjual_dari_live INT, pembeli INT, harga_rata_rata NUMERIC,
            cvr_live DOUBLE, penonton INT, live_stream_dilihat INT, durasi_tonton_rata INT,
            komentar INT, live_dibagikan INT, suka_pada_live INT, follower_baru INT, produk_dilihat INT,
            klik_produk INT, ctr_live DOUBLE, snapshot_ts VARCHAR, snapshot_date DATE, run_id VARCHAR,
            row_hash_raw VARCHAR, row_hash_clean VARCHAR
        );
    """)
    con.sql("""
        CREATE MACRO DATETIME(d, t) AS TRY_CAST(d || ' ' || t AS TIMESTAMP);
    """)

    # 4. Read & Transpile SQL
    print("[SILVER] Running MERGE into SILVER_DB.silver_tt_live ...")
    root_dir = Path(__file__).resolve().parents[3]  # Path ke root etl-data-produk/
    sql_path = root_dir / "sql" / "silver_merge_tt_live.sql"

    sql_content = sql_path.read_text(encoding="utf-8")
    sql_executable = _transpile_bq_to_duckdb(sql_content)

    # 5. Execute MERGE Query
    try:
        con.sql(sql_executable)
        print("✅ MERGE SQL Execution Success!")
    except Exception as e:
        print(f"❌ Error saat eksekusi SQL: {e}")

    print("[SILVER] MERGE DONE")
    print("Show Sampel Data silver_tt_live:")
    con.sql("SELECT * FROM SILVER_DB.silver_tt_live LIMIT 3").show()
    con.close()
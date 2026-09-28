# src/data_live/utils/recovery.py
"""Error-recovery path-A logic: select rows from resolved / changed errors."""

import pandas as pd


def _waktu_live_present(df: pd.DataFrame) -> pd.Series:
    """Boolean mask of rows that build_bronze_live will actually keep.

    build_bronze_live hard-drops rows with a blank `waktu_live`
    (clean_bronze.py). Applying the same test here keeps the recovery counters
    honest: a row that is about to be dropped is not reported as re-admitted.
    This does not change what reaches bronze.
    """
    for col in ("Waktu Live", "waktu_live"):
        if col in df.columns:
            return df[col].astype(str).str.strip() != ""
    return pd.Series(True, index=df.index)


def select_recovered(
    df_valid: pd.DataFrame,
    resolved: list,
    report: dict,
    readmit: list | None = None,
) -> pd.DataFrame:
    """Select rows from *df_valid* that belong to a recovered error group.

    Grain is (sheet_name, creds, toko, error_date) — toko verbatim. Two kinds of
    group are passed in, and both are treated identically: select EVERY valid row
    carrying the group's key.

      - `resolved`: the group is no longer detected as bad this run (defect gone,
        or a date_future / legacy future-date entry remediated).
      - `readmit` : the group is still detected, but its signature
        (n_rows, error_reasons) changed — some rows were fixed, or the defect
        changed shape.

    Re-admitting a whole group is deliberately NOT count-matched. `row_hash_raw`
    is built from (waktu_live, toko, id_kreator, produk_yang_ditambahkan,
    produk_dilihat) and therefore excludes `Tanggal` and every numeric column, so
    a re-admitted row hashes identically to its twin already in bronze. The
    bronze idempotency gate drops those; only genuinely fixed rows survive to be
    appended. This is what lets a PARTIALLY fixed date recover — a strict
    `count == n_rows` test could never fire, because fixing some rows lowers the
    count and the group stays open forever.

    Callers must pass these rows through build_bronze_live with an EMPTY
    watermark map so the watermark date cannot gate them; the returned frame
    bypasses the regular incremental path.

    Counters added to ``report``:
      recovery_resolved        : fully resolved groups considered
      recovery_partial         : still-broken groups whose signature changed
      recovery_readmit_rows    : rows selected for Path A
      recovery_absent          : resolved groups with no matching valid rows
    """
    df = df_valid.copy()

    candidates = list(resolved or []) + list(readmit or [])

    if df.empty or not candidates:
        report.setdefault("recovery_resolved", 0)
        report.setdefault("recovery_partial", 0)
        report.setdefault("recovery_readmit_rows", 0)
        report.setdefault("recovery_absent", 0)
        return df.iloc[0:0]

    if "Toko" in df.columns:
        toko_series = df["Toko"].astype(str)
    elif "toko" in df.columns:
        toko_series = df["toko"].astype(str)
    else:
        toko_series = pd.Series("", index=df.index, dtype=str)

    try:
        tanggal_str = df["Tanggal"].dt.date.astype(str)
    except Exception:
        tanggal_str = pd.to_datetime(df["Tanggal"]).dt.date.astype(str)

    key_series = (
        df["sheet_name"].astype(str)
        + "|" + df["creds"].astype(str)
        + "|" + toko_series
        + "|" + tanggal_str
    )

    match = pd.Series(False, index=df.index)
    absent = 0

    for r in candidates:
        key = f'{r["sheet_name"]}|{r["creds"]}|{r.get("toko") or ""}|{r["error_date"]}'
        grp = df.index[key_series == key]
        if len(grp) == 0:
            absent += 1
        else:
            match.loc[grp] = True

    # Drop rows build_bronze_live would discard anyway, so the reported count
    # matches what can actually be appended.
    match = match & _waktu_live_present(df)

    report["recovery_resolved"] = len(resolved or [])
    report["recovery_partial"] = len(readmit or [])
    report["recovery_readmit_rows"] = int(match.sum())
    report["recovery_absent"] = absent

    return df[match]

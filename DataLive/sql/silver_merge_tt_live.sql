MERGE `database-sigma.SILVER_DB.silver_tt_live` T
USING (
  WITH latest_raw AS (
    SELECT * EXCEPT(rn) FROM (
      SELECT b.*,
             ROW_NUMBER() OVER (
               PARTITION BY UPPER(TRIM(b.toko)),
                            UPPER(TRIM(b.id_kreator)),
                            DATE(b.tanggal),
                            b.pukul_live
               ORDER BY b.snapshot_ts DESC, b.run_id DESC
             ) rn
      FROM `database-sigma.BRONZE_DB.bronze_live` b
    )
    WHERE rn = 1
  ),
  base AS (
    SELECT
      DATE(tanggal)               AS tanggal,
      UPPER(TRIM(toko))           AS toko,
      UPPER(TRIM(id_kreator))     AS id_kreator,
      UPPER(TRIM(kreator))        AS kreator,
      UPPER(TRIM(nama_panggilan)) AS nama_panggilan,

      SAFE_CAST(pukul_live AS INT64) AS pukul_live,
      CASE
        WHEN SAFE_CAST(pukul_live AS INT64) IS NOT NULL
          THEN TIME(SAFE_CAST(pukul_live AS INT64), 0, 0)
        WHEN SAFE.PARSE_TIME('%H:%M', waktu_live) IS NOT NULL
          THEN SAFE.PARSE_TIME('%H:%M', waktu_live)
        ELSE NULL
      END AS waktu_live_time,
      CASE
        WHEN SAFE_CAST(pukul_live AS INT64) IS NOT NULL
          THEN DATETIME(DATE(tanggal), TIME(SAFE_CAST(pukul_live AS INT64), 0, 0))
        WHEN SAFE.PARSE_TIME('%H:%M', waktu_live) IS NOT NULL
          THEN DATETIME(DATE(tanggal), SAFE.PARSE_TIME('%H:%M', waktu_live))
        ELSE NULL
      END AS sesi_start,

      SAFE_CAST(nilai_barang_dagangan_bruto_live_rp AS NUMERIC) AS gmv_bruto_live,
      SAFE_CAST(gmv_yang_didapat_dari_live_rp AS NUMERIC)       AS gmv_live,

      SAFE_CAST(produk_yang_ditambahkan AS INT64)               AS produk_ditambahkan,
      SAFE_CAST(produk_terjual AS INT64)                         AS produk_terjual,
      SAFE_CAST(pesanan_sku_yang_dibuat AS INT64)                AS pesanan_sku_dibuat,
      SAFE_CAST(pesanan_sku_live AS INT64)                       AS pesanan_sku_live,
      SAFE_CAST(produk_yang_terjual_dari_live AS INT64)          AS produk_terjual_dari_live,
      SAFE_CAST(pembeli AS INT64)                                AS pembeli,
      SAFE_CAST(harga_ratarata_rp AS NUMERIC)                    AS harga_rata_rata,

      SAFE_CAST(REGEXP_REPLACE(rasio_pesanan_per_klik_live, r'[%\\s]', '') AS FLOAT64)/100 AS cvr_live,
      SAFE_CAST(penonton AS INT64)                          AS penonton,
      SAFE_CAST(live_stream_dilihat AS INT64)               AS live_stream_dilihat,
      SAFE_CAST(durasi_menonton_ratarata_siaran_live AS INT64) AS durasi_tonton_rata,
      SAFE_CAST(komentar AS INT64)                          AS komentar,
      SAFE_CAST(live_dibagikan AS INT64)                    AS live_dibagikan,
      SAFE_CAST(suka_pada_live AS INT64)                    AS suka_pada_live,
      SAFE_CAST(pengikut_baru_video_kreator AS INT64)       AS follower_baru,
      SAFE_CAST(produk_dilihat AS INT64)                    AS produk_dilihat,
      SAFE_CAST(klik_produk AS INT64)                       AS klik_produk,
      SAFE_CAST(REGEXP_REPLACE(ctr, r'[%\\s]', '') AS FLOAT64)/100 AS ctr_live,

      snapshot_ts, snapshot_date, run_id, row_hash_raw
    FROM latest_raw
  ),
  with_hash AS (
    SELECT
      b.*,
      TO_HEX(SHA256(
        ARRAY_TO_STRING([
          FORMAT_DATE('%F', b.tanggal),
          COALESCE(b.toko,''), COALESCE(b.id_kreator,''), COALESCE(b.kreator,''), COALESCE(b.nama_panggilan,''),
          CAST(COALESCE(b.pukul_live, -1) AS STRING),
          CAST(b.waktu_live_time AS STRING),
          CAST(b.sesi_start AS STRING),
          CAST(b.gmv_bruto_live AS STRING),
          CAST(b.gmv_live AS STRING),
          CAST(b.produk_ditambahkan AS STRING),
          CAST(b.produk_terjual AS STRING),
          CAST(b.pesanan_sku_dibuat AS STRING),
          CAST(b.pesanan_sku_live AS STRING),
          CAST(b.produk_terjual_dari_live AS STRING),
          CAST(b.pembeli AS STRING),
          CAST(b.harga_rata_rata AS STRING),
          CAST(b.cvr_live AS STRING),
          CAST(b.penonton AS STRING),
          CAST(b.live_stream_dilihat AS STRING),
          CAST(b.durasi_tonton_rata AS STRING),
          CAST(b.komentar AS STRING),
          CAST(b.live_dibagikan AS STRING),
          CAST(b.suka_pada_live AS STRING),
          CAST(b.follower_baru AS STRING),
          CAST(b.produk_dilihat AS STRING),
          CAST(b.klik_produk AS STRING),
          CAST(b.ctr_live AS STRING)
        ], '||')
      )) AS row_hash_clean
    FROM base b
  )
  SELECT * FROM with_hash
) S
ON  T.tanggal    = S.tanggal
AND T.toko       = S.toko
AND T.id_kreator = S.id_kreator
AND ( (T.pukul_live IS NOT NULL AND S.pukul_live IS NOT NULL AND T.pukul_live = S.pukul_live)
   OR (T.pukul_live IS NULL AND S.pukul_live IS NULL AND T.waktu_live_time = S.waktu_live_time) )
WHEN MATCHED AND T.row_hash_clean != S.row_hash_clean THEN
  UPDATE SET
    kreator = S.kreator,
    nama_panggilan = S.nama_panggilan,
    pukul_live = S.pukul_live,
    waktu_live_time = S.waktu_live_time,
    sesi_start = S.sesi_start,
    gmv_bruto_live = S.gmv_bruto_live,
    gmv_live = S.gmv_live,
    produk_ditambahkan = S.produk_ditambahkan,
    produk_terjual = S.produk_terjual,
    pesanan_sku_dibuat = S.pesanan_sku_dibuat,
    pesanan_sku_live = S.pesanan_sku_live,
    produk_terjual_dari_live = S.produk_terjual_dari_live,
    pembeli = S.pembeli,
    harga_rata_rata = S.harga_rata_rata,
    cvr_live = S.cvr_live,
    penonton = S.penonton,
    live_stream_dilihat = S.live_stream_dilihat,
    durasi_tonton_rata = S.durasi_tonton_rata,
    komentar = S.komentar,
    live_dibagikan = S.live_dibagikan,
    suka_pada_live = S.suka_pada_live,
    follower_baru = S.follower_baru,
    produk_dilihat = S.produk_dilihat,
    klik_produk = S.klik_produk,
    ctr_live = S.ctr_live,
    snapshot_ts = S.snapshot_ts, snapshot_date = S.snapshot_date, run_id = S.run_id,
    row_hash_raw = S.row_hash_raw, row_hash_clean = S.row_hash_clean
WHEN NOT MATCHED THEN
  INSERT (
    tanggal, toko, id_kreator, kreator, nama_panggilan,
    pukul_live, waktu_live_time, sesi_start,
    gmv_bruto_live, gmv_live, produk_ditambahkan, produk_terjual,
    pesanan_sku_dibuat, pesanan_sku_live, produk_terjual_dari_live, pembeli,
    harga_rata_rata, cvr_live, penonton, live_stream_dilihat, durasi_tonton_rata,
    komentar, live_dibagikan, suka_pada_live, follower_baru, produk_dilihat,
    klik_produk, ctr_live,
    snapshot_ts, snapshot_date, run_id, row_hash_raw, row_hash_clean
  )
  VALUES (
    S.tanggal, S.toko, S.id_kreator, S.kreator, S.nama_panggilan,
    S.pukul_live, S.waktu_live_time, S.sesi_start,
    S.gmv_bruto_live, S.gmv_live, S.produk_ditambahkan, S.produk_terjual,
    S.pesanan_sku_dibuat, S.pesanan_sku_live, S.produk_terjual_dari_live, S.pembeli,
    S.harga_rata_rata, S.cvr_live, S.penonton, S.live_stream_dilihat, S.durasi_tonton_rata,
    S.komentar, S.live_dibagikan, S.suka_pada_live, S.follower_baru, S.produk_dilihat,
    S.klik_produk, S.ctr_live,
    S.snapshot_ts, S.snapshot_date, S.run_id, S.row_hash_raw, S.row_hash_clean
  );
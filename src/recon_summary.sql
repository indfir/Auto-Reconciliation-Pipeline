-- =============================================================================
-- recon_summary.sql
-- =============================================================================
-- Summary reconciliation query that joins three data sources:
--   A  = Production system (internal transaction records)
--   B  = Issuer report    (transaction data from each payment issuer)
--   G  = Bank file        (actual money received in company bank accounts)
--
-- Output columns (matching dashboard):
--   A   : YTI Traffic          — transaction count from production system
--   B   : Issuer Traffic       — transaction count from issuer report
--   C   : YTI Trx Amount       — gross transaction value (production)
--   D   : Issuer Trx Amount    — gross transaction value (issuer)
--   E   : YTI Settlement Amount— net amount after MDR (production)
--   F   : Issuer Settlement    — net amount after MDR (issuer)
--   G   : Amount Received      — actual money received in bank account
--   A-B : Gap Traffic          — count variance production vs issuer
--   C-D : Gap Trx Amount       — amount variance production vs issuer
--   G-F : Gap Received vs Issuer— cash flow vs issuer settlement
--   G-E : Gap Received vs YTI  — cash flow vs production settlement
--
-- SETUP: Replace placeholders:
--   your-project-id   → your GCP project ID
--   your_dataset      → your BigQuery dataset name
--   your_prod_dataset → your production data dataset name
-- =============================================================================

WITH

-- =============================================================================
-- DATE SPINE
-- Generates one row per (date, issuer/bank combination) for 9 months back.
-- This ensures every combination appears in the output even on days with
-- zero transactions.
-- =============================================================================
DATE_ARRAY AS (
  WITH date_table AS (
    SELECT receival_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
      DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH),
      CURRENT_DATE()
    )) AS receival_date
  ),
  -- Add or remove issuers here to match your integration list
  issuer_table AS (
    SELECT 'ShopeePay' AS bank, 'ShopeePay'            AS remark, 'ShopeePay - ShopeePay'                     AS issuer_name UNION ALL
    SELECT 'OVO',               'OVO',                             'OVO - OVO'                                                   UNION ALL
    SELECT 'OTTOPAY',           'OTTOPAY',                         'OTTOPAY - OTTOPAY'                                           UNION ALL
    SELECT 'Mandiri',           'MCD',                             'Mandiri - MCD'                                               UNION ALL
    SELECT 'Mandiri',           'FM',                              'Mandiri - FM'                                                UNION ALL
    SELECT 'Mandiri',           'ELSE',                            'Mandiri - ELSE'                                              UNION ALL
    SELECT 'LinkAja',           'MSME PM (10.00-21.59)',           'LinkAja - MSME PM (10.00-21.59)'                            UNION ALL
    SELECT 'LinkAja',           'MSME AM (22.00-09.59)',           'LinkAja - MSME AM (22.00-09.59)'                            UNION ALL
    SELECT 'LinkAja',           'MCD PM (10.00-21.59)',            'LinkAja - MCD PM (10.00-21.59)'                             UNION ALL
    SELECT 'LinkAja',           'MCD AM (22.00-09.59)',            'LinkAja - MCD AM (22.00-09.59)'                             UNION ALL
    SELECT 'LinkAja',           'Expansion (D-1)',                 'LinkAja - Expansion (D-1)'                                  UNION ALL
    SELECT 'Kredivo',           'Willfitness / Other',             'Kredivo - Willfitness / Other'                              UNION ALL
    SELECT 'Kredivo',           'MCD',                             'Kredivo - MCD'                                              UNION ALL
    SELECT 'Kredivo',           'HERO',                            'Kredivo - HERO'                                             UNION ALL
    SELECT 'Kredivo',           'FM',                              'Kredivo - FM'                                               UNION ALL
    SELECT 'INDODANA',          'INDODANA',                        'INDODANA - INDODANA'                                        UNION ALL
    SELECT 'HANA',              'HANA',                            'HANA - HANA'                                                UNION ALL
    SELECT 'BTN',               'BTN',                             'BTN - BTN'                                                  UNION ALL
    SELECT 'BNI',               'OTO - MSME',                      'BNI - OTO - MSME'                                          UNION ALL
    SELECT 'BNI',               'OTO - MCD',                       'BNI - OTO - MCD'                                           UNION ALL
    SELECT 'BNI',               'OTO - HERO',                      'BNI - OTO - HERO'                                          UNION ALL
    SELECT 'BNI',               'OTO - FM',                        'BNI - OTO - FM'                                            UNION ALL
    SELECT 'BCA',               'BCA',                             'BCA - BCA'
  )
  SELECT d.receival_date, t.bank, t.remark, t.issuer_name
  FROM date_table d CROSS JOIN issuer_table t
),

-- Helper CTEs for remark groupings used in cross-join fallbacks
REMARKS_OTO AS (
  SELECT 'OTO - MSME' AS remarks UNION ALL SELECT 'OTO - MCD'
  UNION ALL SELECT 'OTO - HERO'  UNION ALL SELECT 'OTO - FM'
),
REMARKS_MANDIRI AS (
  SELECT 'FM' AS remarks UNION ALL SELECT 'MCD' UNION ALL SELECT 'ELSE'
),


-- =============================================================================
-- SOURCE A: PRODUCTION SYSTEM
-- =============================================================================
-- Reads from the internal hourly settlement report.
-- Key transformations applied here:
--   1. Date adjustment: some issuers batch transactions that cross midnight;
--      the adjusted_date shifts cut-off transactions to the correct receival date.
--   2. Merchant categorization: maps each transaction to an issuer-specific
--      merchant category (MCD / FM / HERO / MSME / etc.) based on merchant
--      attributes stored in the merchants master table.
--   3. MDR rate lookup: each issuer has a rate schedule; on/off-us rates may
--      differ. Rates are resolved per transaction at query time.
-- =============================================================================
SETTLEMENT_REPORT AS (
  SELECT
    DATE(txn_date)                AS txn_date,
    TIME(txn_date)                AS txn_time,

    -- Date adjustment: shift transactions that cross the day boundary
    -- to the correct settlement date for each issuer.
    -- Each issuer has its own cut-off time (e.g. 22:00, 23:30).
    -- Replace the CASE block conditions with your own issuer cut-off rules.
    CASE
      WHEN issuer = 'YOUR_ISSUER_A'
       AND TIME(txn_date) >= 'YOUR_CUTOFF_TIME'
        THEN DATE_ADD(DATE(txn_date), INTERVAL 1 DAY)
      WHEN issuer = 'YOUR_ISSUER_B'
       AND TIME(txn_date) >= 'YOUR_CUTOFF_TIME'
        THEN DATE_ADD(DATE(txn_date), INTERVAL 1 DAY)
      ELSE DATE(txn_date)
    END AS adjusted_date,

    s.issuer,
    sm.account_id,
    merchant_id,
    merch_name,
    referral_code_acquisition,
    txn_type,

    -- Merchant category: map each issuer's transactions to a business segment.
    -- Replace the conditions below with your own merchant classification logic
    -- (referral codes, vendor types, account IDs, etc.).
    CASE
      WHEN s.issuer = 'YOUR_ISSUER_A' THEN
        CASE
          WHEN referral_code_acquisition = 'YOUR_SEGMENT_CODE_1' THEN 'Segment 1'
          WHEN referral_code_acquisition = 'YOUR_SEGMENT_CODE_2' THEN 'Segment 2'
          ELSE 'Other'
        END
      WHEN s.issuer = 'YOUR_ISSUER_B' THEN
        CASE
          WHEN sm.vendor_type = 'YOUR_VENDOR_TYPE_1' THEN 'MCD'
          WHEN sm.vendor_type = 'YOUR_VENDOR_TYPE_2' THEN 'FM'
          ELSE 'Other'
        END
      -- Add more issuers here
      ELSE 'Unknown'
    END AS merchant_category,

    -- Remarks for issuers that split by time window (e.g. AM/PM batches)
    CASE
      WHEN s.issuer = 'YOUR_TIME_SPLIT_ISSUER' THEN
        CASE
          WHEN TIME(txn_date) >= '22:00:00' OR TIME(txn_date) < '10:00:00'
            THEN 'AM Batch (22:00-09:59)'
          ELSE
            'PM Batch (10:00-21:59)'
        END
    END AS remarks_time_split,

    -- Reference number: use internal or issuer reference depending on date range
    -- Replace the date and field names with your own logic
    IF(
      DATETIME(txn_date) < DATETIME('YOUR_MIGRATION_DATE'),
      internal_ref_field,
      issuer_ref_field
    ) AS txn_ref,

    issuer_txn_ref,
    credit_trans_trx_id,
    s.terminal_id,
    st.onus_offus,

    -- MDR rate per issuer/merchant type.
    -- Replace the CASE conditions with your actual rate schedule.
    CASE
      WHEN s.issuer = 'YOUR_ISSUER_A' THEN
        IF(onus_offus = 'OFF US', 0.0054, 0.0035)  -- example on/off-us rates
      WHEN s.issuer = 'YOUR_ISSUER_B' THEN
        0.007  -- flat rate example
      -- Add more issuers here
      ELSE 0.007
    END AS mdr,

    amt AS gmv

  FROM `your-project-id.your_prod_dataset.settlement_report_hourly` s
  LEFT JOIN `your-project-id.your_prod_dataset.reversal_trx_reference`    b  ON s.credit_trans_trx_id = b.trx_id
  LEFT JOIN `your-project-id.your_prod_dataset.merchants`                 sm ON s.account_id = sm.account_id
  LEFT JOIN (
    SELECT trx_id, account_id, onus_offus
    FROM `your-project-id.your_prod_dataset.transactions`
    WHERE DATE(trx_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
  ) st ON CAST(s.credit_trans_trx_id AS STRING) = st.trx_id

  -- Exclude test/non-production issuers
  WHERE s.issuer NOT IN ('YOUR_TEST_ISSUER_1', 'YOUR_TEST_ISSUER_2')
  AND DATE(txn_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
  -- Exclude test merchant accounts; replace IDs with your own exclusion list
  AND sm.account_id NOT IN (/* YOUR_EXCLUDED_ACCOUNT_IDS */)
),


-- =============================================================================
-- SOURCE G: BANK FILES — Actual money received in company bank accounts
-- =============================================================================
-- Each CTE reads from one bank's mutation/statement table loaded by the Airflow
-- DAG. The key transformations:
--   - Parse the description field to identify which issuer made the transfer
--   - Apply a D-1 date shift (most banks settle on T+1)
-- =============================================================================

FILE_BANK_BTN AS (
  SELECT
    post_date                 AS trx_date,
    DATE(post_date)           AS receival_date,
    description,
    amount_credit             AS amount_received
  FROM `your-project-id.your_dataset.recon_btn_bank_file_mutation`
),

FILE_BANK_BCA AS (
  SELECT
    transaction_date          AS trx_date,
    DATE(transaction_date)    AS receival_date,
    description,
    branch,
    transaction_amount        AS amount_received
  FROM `your-project-id.your_dataset.recon_bca_bank_file_report`
),

FILE_BANK_MANDIRI AS (
  SELECT DISTINCT
    postdate                  AS trx_datetime,
    DATE(postdate)            AS receival_date,
    LEFT(remarks, 11)         AS MID,
    -- Classify merchant segment from trading name
    -- Replace the LIKE conditions with your own merchant name patterns
    CASE
      WHEN UPPER(b.trading_name) LIKE ANY ('%YOUR_SEGMENT_1%') THEN 'Segment 1'
      WHEN UPPER(b.trading_name) LIKE ANY ('%YOUR_SEGMENT_2%') THEN 'Segment 2'
      ELSE 'ELSE'
    END AS remarks,
    remarks                   AS remark,
    credit_amount             AS amount_received
  FROM `your-project-id.your_dataset.recon_mandiri_bank_file_report` bf
  LEFT JOIN (
    SELECT DISTINCT MID, trading_name
    FROM `your-project-id.your_dataset.recon_mandiri_bank_report`
  ) b ON LEFT(bf.remarks, 11) = b.mid
  -- Filter: only include rows related to your QRIS settlement
  -- Replace 'YOUR_SETTLEMENT_KEYWORD' with the keyword in your Mandiri remittance description
  WHERE remarks LIKE '%YOUR_SETTLEMENT_KEYWORD%'
),

FILE_BANK_BNI AS (
  SELECT
    DATE(PARSE_TIMESTAMP(
      CASE
        WHEN REGEXP_CONTAINS(post_date, r"^\d{2}/\d{2}/\d{2} \d{2}.\d{2}$") THEN "%d/%m/%y %H.%M"
        ELSE "%d/%m/%y %H.%M.%S"
      END,
      post_date
    )) AS receival_date,
    description,
    -- Classify transaction by description keyword
    -- Replace 'YOUR_KEYWORD_A', 'YOUR_KEYWORD_B' with your actual bank description patterns
    CASE
      WHEN UPPER(description) LIKE '%YOUR_KEYWORD_A%' THEN
        CASE
          WHEN UPPER(description) LIKE '%SEGMENT_1%' THEN 'Category - Segment 1'
          WHEN UPPER(description) LIKE '%SEGMENT_2%' THEN 'Category - Segment 2'
          ELSE 'Category - Other'
        END
      WHEN UPPER(description) LIKE '%YOUR_KEYWORD_B%' THEN
        CASE
          WHEN UPPER(description) LIKE '%SEGMENT_1%' THEN 'OTO - Segment 1'
          ELSE 'OTO - Other'
        END
      ELSE 'Other'
    END AS remarks,
    credit AS amount_received
  FROM `your-project-id.your_dataset.recon_bni_bank_file_report`
),

FILE_BANK_LINKAJA AS (
  -- LinkAja receives funds via two channels: BNI (MSME/MCD) and BCA (Expansion D-1)
  WITH BNI_LINKAJA AS (
    SELECT
      post_date,
      DATE(PARSE_TIMESTAMP(
        CASE
          WHEN REGEXP_CONTAINS(post_date, r"^\d{2}/\d{2}/\d{2} \d{2}.\d{2}$") THEN "%d/%m/%y %H.%M"
          ELSE "%d/%m/%y %H.%M.%S"
        END,
        post_date
      )) AS receival_date,
      credit AS amount_received,
      -- Classify by hour window; threshold amount distinguishes MCD vs MSME
      -- Replace 10000000 with your actual threshold value
      CASE
        WHEN EXTRACT(HOUR FROM PARSE_TIMESTAMP(
          CASE WHEN REGEXP_CONTAINS(post_date, r"^\d{2}/\d{2}/\d{2} \d{2}.\d{2}$")
               THEN "%d/%m/%y %H.%M" ELSE "%d/%m/%y %H.%M.%S" END, post_date
        )) BETWEEN 10 AND 21 THEN
          IF(credit < YOUR_MCD_THRESHOLD_AMOUNT, 'MCD AM (22.00-09.59)', 'MSME AM (22.00-09.59)')
        ELSE
          IF(credit < YOUR_MCD_THRESHOLD_AMOUNT, 'MCD PM (10.00-21.59)', 'MSME PM (10.00-21.59)')
      END AS remarks
    FROM `your-project-id.your_dataset.recon_bni_bank_file_report`
    -- Replace 'YOUR_LINKAJA_DESCRIPTION_KEYWORD' with the LinkAja identifier in BNI descriptions
    WHERE description LIKE '%YOUR_LINKAJA_DESCRIPTION_KEYWORD%'
  ),
  BCA_EXPANSION AS (
    SELECT
      trx_date,
      DATE_SUB(DATE(receival_date), INTERVAL 1 DAY) AS receival_date,
      'Expansion (D-1)' AS remarks,
      amount_received
    FROM FILE_BANK_BCA
    -- Replace 'YOUR_EXPANSION_KEYWORD' with the keyword in your BCA expansion description
    WHERE description LIKE '%YOUR_EXPANSION_KEYWORD%'
  )
  SELECT receival_date, remarks, amount_received FROM BNI_LINKAJA
  UNION ALL
  SELECT receival_date, remarks, amount_received FROM BCA_EXPANSION
),


-- =============================================================================
-- CROSS-JOIN STUBS
-- Ensure BNI/Mandiri sub-segments always appear in the output even when
-- no data exists (avoids missing rows in the dashboard).
-- =============================================================================
BNI_CROSS AS (
  SELECT
    da.receival_date, 'BNI' AS bank, remarks,
    CAST(NULL AS NUMERIC) AS yti_traffic,
    CAST(NULL AS NUMERIC) AS yti_trx_amount,
    CAST(NULL AS NUMERIC) AS yti_settlement_amount,
    CAST(NULL AS NUMERIC) AS issuer_traffic,
    CAST(NULL AS NUMERIC) AS issuer_trx_amount,
    CAST(NULL AS NUMERIC) AS issuer_settlement_amount,
    CAST(NULL AS NUMERIC) AS amount_received
  FROM DATE_ARRAY da CROSS JOIN REMARKS_OTO
),
MANDIRI_CROSS AS (
  SELECT
    da.receival_date, 'Mandiri' AS bank, remarks,
    CAST(NULL AS NUMERIC) AS yti_traffic,
    CAST(NULL AS NUMERIC) AS yti_trx_amount,
    CAST(NULL AS NUMERIC) AS yti_settlement_amount,
    CAST(NULL AS NUMERIC) AS issuer_traffic,
    CAST(NULL AS NUMERIC) AS issuer_trx_amount,
    CAST(NULL AS NUMERIC) AS issuer_settlement_amount,
    CAST(NULL AS NUMERIC) AS amount_received
  FROM DATE_ARRAY da CROSS JOIN REMARKS_MANDIRI
),


-- =============================================================================
-- SOURCE B: ISSUER REPORTS
-- =============================================================================
-- One CTE per issuer reads the formatted data loaded by the Airflow DAG.
-- Each CTE aggregates to (date, merchant_segment) granularity and outputs
-- issuer_traffic + issuer_trx_amount + issuer_settlement_amount.
-- =============================================================================

ISSUER_KREDIVO AS (
  SELECT
    DATE(transaction_date)   AS receival_date,
    'Kredivo'                AS bank,
    -- Replace field and value with your issuer's merchant name field & values
    CASE
      WHEN LOWER(store_name) = 'YOUR_MERCHANT_1' THEN 'MCD'
      WHEN LOWER(store_name) = 'YOUR_MERCHANT_2' THEN 'FM'
      WHEN LOWER(store_name) = 'YOUR_MERCHANT_3' THEN 'HERO'
      ELSE 'Other'
    END AS remarks,
    CAST(NULL AS NUMERIC) AS yti_traffic,
    CAST(NULL AS NUMERIC) AS yti_trx_amount,
    CAST(NULL AS NUMERIC) AS yti_settlement_amount,
    COUNT(DISTINCT transaction_id) AS issuer_traffic,
    SUM(amount)                    AS issuer_trx_amount,
    CAST(NULL AS NUMERIC)          AS issuer_settlement_amount,
    CAST(NULL AS NUMERIC)          AS amount_received
  FROM `your-project-id.your_dataset.recon_kredivo_report`
  WHERE status = 'Settled'
  AND DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
  GROUP BY 1, 2, 3
),

ISSUER_INDODANA AS (
  SELECT
    DATE(transaction_date)   AS receival_date,
    'INDODANA'               AS bank,
    'INDODANA'               AS remarks,
    CAST(NULL AS NUMERIC) AS yti_traffic,
    CAST(NULL AS NUMERIC) AS yti_trx_amount,
    CAST(NULL AS NUMERIC) AS yti_settlement_amount,
    COUNT(DISTINCT transidmerchant) AS issuer_traffic,
    SUM(CASE WHEN transaction_type = 'Purchase' THEN amount ELSE -amount END)           AS issuer_trx_amount,
    SUM(CASE WHEN transaction_type = 'Purchase' THEN pay_to_merchant ELSE -pay_to_merchant END) AS issuer_settlement_amount,
    CAST(NULL AS NUMERIC) AS amount_received
  FROM `your-project-id.your_dataset.recon_indodana_report`
  WHERE DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
  GROUP BY 1, 2, 3
),

ISSUER_OVO AS (
  SELECT
    DATE(transactiondate)    AS receival_date,
    'OVO'                    AS bank,
    'OVO'                    AS remarks,
    CAST(NULL AS NUMERIC) AS yti_traffic,
    CAST(NULL AS NUMERIC) AS yti_trx_amount,
    CAST(NULL AS NUMERIC) AS yti_settlement_amount,
    COUNT(DISTINCT merchantinvoice) AS issuer_traffic,
    SUM(transactionamount)          AS issuer_trx_amount,
    SUM(nettsettlement)             AS issuer_settlement_amount,
    CAST(NULL AS NUMERIC) AS amount_received
  FROM `your-project-id.your_dataset.recon_ovo_report`
  WHERE transactiontype != 'Void'
  AND DATE(transactiondate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
  GROUP BY 1, 2, 3
),

ISSUER_SHOPEEPAY AS (
  SELECT
    DATE(create_time)        AS receival_date,
    'ShopeePay'              AS bank,
    'ShopeePay'              AS remarks,
    CAST(NULL AS NUMERIC) AS yti_traffic,
    CAST(NULL AS NUMERIC) AS yti_trx_amount,
    CAST(NULL AS NUMERIC) AS yti_settlement_amount,
    COUNT(DISTINCT transaction_id) AS issuer_traffic,
    SUM(transaction_amount)        AS issuer_trx_amount,
    SUM(settlement_amount)         AS issuer_settlement_amount,
    CAST(NULL AS NUMERIC) AS amount_received
  FROM `your-project-id.your_dataset.recon_shopeepay_report`
  WHERE transaction_type = 'Payment'
  AND DATE(create_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
  GROUP BY 1, 2, 3
),

ISSUER_MANDIRI AS (
  SELECT
    DATE(trxdate)            AS receival_date,
    'Mandiri'                AS bank,
    -- Merchant categorization from Mandiri issuer report
    CASE
      WHEN UPPER(b.trading_name) LIKE ANY ('%YOUR_SEGMENT_1_PATTERN%') THEN 'MCD'
      WHEN UPPER(b.trading_name) LIKE ANY ('%YOUR_SEGMENT_2_PATTERN%') THEN 'FM'
      ELSE 'ELSE'
    END AS remarks,
    CAST(NULL AS NUMERIC) AS yti_traffic,
    CAST(NULL AS NUMERIC) AS yti_trx_amount,
    CAST(NULL AS NUMERIC) AS yti_settlement_amount,
    COUNT(DISTINCT reff_id)  AS issuer_traffic,
    SUM(amount)              AS issuer_trx_amount,
    SUM(net_amount)          AS issuer_settlement_amount,
    CAST(NULL AS NUMERIC) AS amount_received
  FROM `your-project-id.your_dataset.recon_mandiri_bank_report` b
  WHERE DATE(trxdate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
  GROUP BY 1, 2, 3
),

ISSUER_BNI AS (
  SELECT
    DATE(trx_datetime)       AS receival_date,
    'BNI'                    AS bank,
    CASE
      WHEN UPPER(nama_merchant) LIKE '%YOUR_MERCHANT_1%' THEN 'OTO - MCD'
      WHEN UPPER(nama_merchant) LIKE '%YOUR_MERCHANT_2%' THEN 'OTO - HERO'
      WHEN UPPER(nama_merchant) LIKE '%YOUR_MERCHANT_3%' THEN 'OTO - FM'
      ELSE 'OTO - MSME'
    END AS remarks,
    CAST(NULL AS NUMERIC) AS yti_traffic,
    CAST(NULL AS NUMERIC) AS yti_trx_amount,
    CAST(NULL AS NUMERIC) AS yti_settlement_amount,
    COUNT(DISTINCT bill_number) AS issuer_traffic,
    SUM(nominal)                AS issuer_trx_amount,
    SUM(net_amount)             AS issuer_settlement_amount,
    CAST(NULL AS NUMERIC) AS amount_received
  FROM `your-project-id.your_dataset.recon_bni_bank_report`
  WHERE status = 'success'
  AND DATE(trx_datetime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
  GROUP BY 1, 2, 3
),

ISSUER_BCA AS (
  SELECT
    DATE(payment_date)       AS receival_date,
    'BCA'                    AS bank,
    'BCA'                    AS remarks,
    CAST(NULL AS NUMERIC) AS yti_traffic,
    CAST(NULL AS NUMERIC) AS yti_trx_amount,
    CAST(NULL AS NUMERIC) AS yti_settlement_amount,
    COUNT(DISTINCT reference_no) AS issuer_traffic,
    SUM(base_amount)             AS issuer_trx_amount,
    SUM(nett)                    AS issuer_settlement_amount,
    CAST(NULL AS NUMERIC) AS amount_received
  FROM `your-project-id.your_dataset.recon_issuer_bca_bank_file_report`
  WHERE DATE(payment_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
  GROUP BY 1, 2, 3
),

ISSUER_BTN AS (
  SELECT
    DATE(transaction_date)   AS receival_date,
    'BTN'                    AS bank,
    'BTN'                    AS remarks,
    CAST(NULL AS NUMERIC) AS yti_traffic,
    CAST(NULL AS NUMERIC) AS yti_trx_amount,
    CAST(NULL AS NUMERIC) AS yti_settlement_amount,
    COUNT(DISTINCT retrieval_reference_number) AS issuer_traffic,
    SUM(amount)                                AS issuer_trx_amount,
    -- Settlement = GMV minus MDR; replace 0.007 with your actual BTN MDR rate
    SUM(amount - (amount * 0.007))             AS issuer_settlement_amount,
    CAST(NULL AS NUMERIC) AS amount_received
  FROM `your-project-id.your_dataset.recon_issuer_btn_bank_file_report`
  WHERE DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
  GROUP BY 1, 2, 3
),

ISSUER_OTTOPAY AS (
  SELECT
    -- OttoPay cuts off at 23:30; transactions after that belong to the next day
    CASE
      WHEN TIME(transaction_time) >= '23:30:00'
        THEN DATE_ADD(DATE(transaction_time), INTERVAL 1 DAY)
      ELSE DATE(transaction_time)
    END AS receival_date,
    'OTTOPAY' AS bank,
    'OTTOPAY' AS remarks,
    CAST(NULL AS NUMERIC) AS yti_traffic,
    CAST(NULL AS NUMERIC) AS yti_trx_amount,
    CAST(NULL AS NUMERIC) AS yti_settlement_amount,
    COUNT(DISTINCT issuer_rrn) AS issuer_traffic,
    SUM(gross_amount)          AS issuer_trx_amount,
    SUM(nett_amount)           AS issuer_settlement_amount,
    CAST(NULL AS NUMERIC) AS amount_received
  FROM `your-project-id.your_dataset.recon_issuer_ottopay_bank_file_report`
  WHERE payment_status = 'Success'
  AND DATE(transaction_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
  GROUP BY 1, 2, 3
),

-- LinkAja issuers (MCD / MSME / Expansion — separate source tables)
ISSUER_LINKAJA_MCD AS (
  SELECT
    CASE
      WHEN TIME(TIMESTAMP(CONCAT(transaction_date, ' ', transaction_time))) >= '22:00:00'
        THEN DATE_ADD(DATE(TIMESTAMP(CONCAT(transaction_date, ' ', transaction_time))), INTERVAL 1 DAY)
      ELSE DATE(TIMESTAMP(CONCAT(transaction_date, ' ', transaction_time)))
    END AS receival_date,
    'LinkAja' AS bank,
    CASE
      WHEN TIME(TIMESTAMP(CONCAT(transaction_date, ' ', transaction_time))) >= '22:00:00'
        OR TIME(TIMESTAMP(CONCAT(transaction_date, ' ', transaction_time))) < '10:00:00'
        THEN 'MCD AM (22.00-09.59)'
      ELSE 'MCD PM (10.00-21.59)'
    END AS remarks,
    CAST(NULL AS NUMERIC) AS yti_traffic,
    CAST(NULL AS NUMERIC) AS yti_trx_amount,
    CAST(NULL AS NUMERIC) AS yti_settlement_amount,
    COUNT(DISTINCT ref_no) AS issuer_traffic,
    SUM(transaction_amount)AS issuer_trx_amount,
    SUM(net_amount)        AS issuer_settlement_amount,
    CAST(NULL AS NUMERIC) AS amount_received
  FROM `your-project-id.your_dataset.recon_linkaja_mcd_report`
  -- Replace 'YOUR_MCD_COMPANY_CODE' with your actual MCD company code value
  WHERE company_code = 'YOUR_MCD_COMPANY_CODE'
  GROUP BY 1, 2, 3
),

ISSUER_LINKAJA_EXPANSION AS (
  SELECT
    DATE(transaction_date)   AS receival_date,
    'LinkAja'                AS bank,
    'Expansion (D-1)'        AS remarks,
    CAST(NULL AS NUMERIC) AS yti_traffic,
    CAST(NULL AS NUMERIC) AS yti_trx_amount,
    CAST(NULL AS NUMERIC) AS yti_settlement_amount,
    COUNT(DISTINCT ref_no) AS issuer_traffic,
    SUM(transaction_amount)AS issuer_trx_amount,
    SUM(net_amount)        AS issuer_settlement_amount,
    CAST(NULL AS NUMERIC) AS amount_received
  FROM `your-project-id.your_dataset.recon_linkaja_expansion_report`
  -- Exclude Telkomsel and forced credit/chargeback rows (adjust to your data)
  WHERE mid != 'YOUR_EXCLUDED_MID'
  AND chargebackoriginalbusinesstransactionid IS NULL
  AND (UPPER(remark) NOT LIKE '%FORCE CREDIT%' OR remark IS NULL)
  AND DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
  GROUP BY 1, 2, 3
),

ISSUER_LINKAJA_MSME AS (
  SELECT
    CASE
      WHEN TIME(TIMESTAMP(CONCAT(transaction_date, ' ', transaction_time))) >= '22:00:00'
        THEN DATE_ADD(DATE(TIMESTAMP(CONCAT(transaction_date, ' ', transaction_time))), INTERVAL 1 DAY)
      ELSE DATE(TIMESTAMP(CONCAT(transaction_date, ' ', transaction_time)))
    END AS receival_date,
    'LinkAja' AS bank,
    CASE
      WHEN TIME(TIMESTAMP(CONCAT(transaction_date, ' ', transaction_time))) >= '22:00:00'
        OR TIME(TIMESTAMP(CONCAT(transaction_date, ' ', transaction_time))) < '10:00:00'
        THEN 'MSME AM (22.00-09.59)'
      ELSE 'MSME PM (10.00-21.59)'
    END AS remarks,
    CAST(NULL AS NUMERIC) AS yti_traffic,
    CAST(NULL AS NUMERIC) AS yti_trx_amount,
    CAST(NULL AS NUMERIC) AS yti_settlement_amount,
    COUNT(DISTINCT ref_no) AS issuer_traffic,
    SUM(transaction_amount)AS issuer_trx_amount,
    SUM(net_amount)        AS issuer_settlement_amount,
    CAST(NULL AS NUMERIC) AS amount_received
  FROM `your-project-id.your_dataset.recon_linkaja_msme_report`
  WHERE mid NOT IN ('YOUR_EXCLUDED_MID_1', 'YOUR_EXCLUDED_MID_2')
  AND chargebackoriginalbusinesstransactionid IS NULL
  AND (UPPER(remark) NOT LIKE '%FORCE CREDIT%' OR remark IS NULL)
  GROUP BY 1, 2, 3
),


-- =============================================================================
-- SOURCE A (continued): PRODUCTION DATA per issuer
-- =============================================================================
-- These CTEs aggregate from SETTLEMENT_REPORT to the same granularity
-- (date + merchant_segment) so they can be UNION ALL'd with issuer/bank data.
-- =============================================================================

-- Template for each YTI CTE (Production side):
-- Replace issuer filter, merchant_category grouping, and MDR logic as needed.

YTI_OTTOPAY AS (
  SELECT adjusted_date AS receival_date, 'OTTOPAY' AS bank, merchant_category AS remarks,
    COUNT(DISTINCT credit_trans_trx_id)                                          AS yti_traffic,
    SUM(CASE WHEN txn_type = 'Customer Purchase' THEN gmv ELSE 0 END)
      - SUM(CASE WHEN txn_type = 'E-money Reversal' THEN gmv ELSE 0 END)        AS yti_trx_amount,
    (SUM(CASE WHEN txn_type = 'Customer Purchase' THEN gmv ELSE 0 END)
      - SUM(CASE WHEN txn_type = 'E-money Reversal' THEN gmv ELSE 0 END))
      * (1 - mdr)                                                                AS yti_settlement_amount,
    NULL AS issuer_traffic, NULL AS issuer_trx_amount, NULL AS issuer_settlement_amount, NULL AS amount_received
  FROM SETTLEMENT_REPORT WHERE issuer = 'YOUR_OTTOPAY_ISSUER_NAME'
  GROUP BY 1, 2, 3, mdr
),
YTI_SHOPEEPAY AS (
  SELECT adjusted_date AS receival_date, 'ShopeePay' AS bank, 'ShopeePay' AS remarks,
    COUNT(DISTINCT credit_trans_trx_id) AS yti_traffic,
    SUM(gmv) AS yti_trx_amount, SUM(gmv - (gmv * mdr)) AS yti_settlement_amount,
    NULL, NULL, NULL, NULL
  FROM SETTLEMENT_REPORT WHERE issuer = 'YOUR_SHOPEEPAY_ISSUER_NAME' GROUP BY 1, 2, 3
),
YTI_OVO AS (
  SELECT adjusted_date AS receival_date, 'OVO' AS bank, 'OVO' AS remarks,
    COUNT(DISTINCT credit_trans_trx_id) AS yti_traffic,
    SUM(gmv) AS yti_trx_amount, SUM(gmv - (gmv * mdr)) AS yti_settlement_amount,
    NULL, NULL, NULL, NULL
  FROM SETTLEMENT_REPORT WHERE issuer = 'YOUR_OVO_ISSUER_NAME' GROUP BY 1, 2, 3
),
YTI_INDODANA AS (
  SELECT adjusted_date AS receival_date, 'INDODANA' AS bank, merchant_category AS remarks,
    COUNT(DISTINCT credit_trans_trx_id) AS yti_traffic,
    SUM(CASE WHEN txn_type = 'Customer Purchase' THEN gmv ELSE 0 END)
      - SUM(CASE WHEN txn_type = 'E-money Reversal' THEN gmv ELSE 0 END) AS yti_trx_amount,
    (SUM(CASE WHEN txn_type = 'Customer Purchase' THEN gmv ELSE 0 END)
      - SUM(CASE WHEN txn_type = 'E-money Reversal' THEN gmv ELSE 0 END)) * (1 - mdr) AS yti_settlement_amount,
    NULL, NULL, NULL, NULL
  FROM SETTLEMENT_REPORT WHERE issuer = 'YOUR_INDODANA_ISSUER_NAME' GROUP BY 1, 2, 3, mdr
),
YTI_KREDIVO AS (
  SELECT adjusted_date AS receival_date, 'Kredivo' AS bank, merchant_category AS remarks,
    COUNT(DISTINCT credit_trans_trx_id) AS yti_traffic,
    SUM(gmv) AS yti_trx_amount, SUM(gmv - (gmv * mdr)) AS yti_settlement_amount,
    NULL, NULL, NULL, NULL
  FROM SETTLEMENT_REPORT WHERE issuer = 'YOUR_KREDIVO_ISSUER_NAME' GROUP BY 1, 2, 3
),
YTI_MANDIRI AS (
  SELECT adjusted_date AS receival_date, 'Mandiri' AS bank, merchant_category AS remarks,
    COUNT(DISTINCT credit_trans_trx_id) AS yti_traffic,
    SUM(gmv) AS yti_trx_amount, SUM(gmv - (gmv * mdr)) AS yti_settlement_amount,
    NULL, NULL, NULL, NULL
  FROM SETTLEMENT_REPORT WHERE issuer = 'YOUR_MANDIRI_ISSUER_NAME' GROUP BY 1, 2, 3
),
YTI_BNI AS (
  SELECT adjusted_date AS receival_date, 'BNI' AS bank,
    CONCAT('OTO - ', merchant_category) AS remarks,
    COUNT(DISTINCT credit_trans_trx_id) AS yti_traffic,
    SUM(gmv) AS yti_trx_amount, SUM(gmv - (gmv * mdr)) AS yti_settlement_amount,
    NULL, NULL, NULL, NULL
  FROM SETTLEMENT_REPORT WHERE issuer = 'YOUR_BNI_ISSUER_NAME' GROUP BY 1, 2, 3
),
YTI_BCA AS (
  SELECT adjusted_date AS receival_date, 'BCA' AS bank, 'BCA' AS remarks,
    COUNT(DISTINCT credit_trans_trx_id) AS yti_traffic,
    SUM(gmv) AS yti_trx_amount, SUM(gmv - (gmv * mdr)) AS yti_settlement_amount,
    NULL, NULL, NULL, NULL
  FROM SETTLEMENT_REPORT WHERE issuer = 'YOUR_BCA_ISSUER_NAME' GROUP BY 1, 2, 3
),
YTI_BTN AS (
  SELECT adjusted_date AS receival_date, 'BTN' AS bank, 'BTN' AS remarks,
    COUNT(DISTINCT credit_trans_trx_id) AS yti_traffic,
    SUM(gmv) AS yti_trx_amount, SUM(gmv - (gmv * mdr)) AS yti_settlement_amount,
    NULL, NULL, NULL, NULL
  FROM SETTLEMENT_REPORT WHERE issuer = 'YOUR_BTN_ISSUER_NAME' GROUP BY 1, 2, 3
),
YTI_LINKAJA_MCD AS (
  SELECT adjusted_date AS receival_date, 'LinkAja' AS bank, remarks_time_split AS remarks,
    COUNT(DISTINCT credit_trans_trx_id) AS yti_traffic,
    SUM(gmv) AS yti_trx_amount, SUM(gmv - (gmv * mdr)) AS yti_settlement_amount,
    NULL, NULL, NULL, NULL
  FROM SETTLEMENT_REPORT
  WHERE merchant_category = 'YOUR_LA_MCD_CATEGORY' AND issuer = 'YOUR_LINKAJA_ISSUER_NAME'
  GROUP BY 1, 2, 3
),
YTI_LINKAJA_EXPANSION AS (
  SELECT adjusted_date AS receival_date, 'LinkAja' AS bank, remarks_time_split AS remarks,
    COUNT(DISTINCT credit_trans_trx_id) AS yti_traffic,
    SUM(gmv) AS yti_trx_amount, SUM(gmv - (gmv * mdr)) AS yti_settlement_amount,
    NULL, NULL, NULL, NULL
  FROM SETTLEMENT_REPORT
  WHERE merchant_category = 'YOUR_LA_EXPANSION_CATEGORY' AND issuer = 'YOUR_LINKAJA_ISSUER_NAME'
  -- Exclude/include specific account IDs for expansion mapping — add your own IDs
  AND account_id NOT IN (/* YOUR_EXCLUDED_ACCOUNT_IDS */)
  GROUP BY 1, 2, 3
),
YTI_LINKAJA_MSME AS (
  SELECT adjusted_date AS receival_date, 'LinkAja' AS bank, remarks_time_split AS remarks,
    COUNT(DISTINCT credit_trans_trx_id) AS yti_traffic,
    SUM(gmv) AS yti_trx_amount, SUM(gmv - (gmv * mdr)) AS yti_settlement_amount,
    NULL, NULL, NULL, NULL
  FROM SETTLEMENT_REPORT
  WHERE merchant_category = 'YOUR_LA_MSME_CATEGORY' AND issuer = 'YOUR_LINKAJA_ISSUER_NAME'
  AND account_id NOT IN (/* YOUR_EXCLUDED_ACCOUNT_IDS */)
  GROUP BY 1, 2, 3
),


-- =============================================================================
-- BANK RECEIVED AMOUNTS (D-1 date shift applied where relevant)
-- =============================================================================
BANK_MANDIRI AS (
  SELECT DATE_SUB(DATE(receival_date), INTERVAL 1 DAY) AS receival_date, 'Mandiri' AS bank, remarks,
    NULL, NULL, NULL, NULL, NULL, NULL, SUM(amount_received) AS amount_received
  FROM FILE_BANK_MANDIRI GROUP BY 1, 2, 3
),
BANK_LINKAJA AS (
  SELECT receival_date, 'LinkAja' AS bank, remarks,
    NULL, NULL, NULL, NULL, NULL, NULL, SUM(amount_received) AS amount_received
  FROM FILE_BANK_LINKAJA GROUP BY 1, 2, 3
),
BANK_BNI AS (
  SELECT DATE_SUB(DATE(receival_date), INTERVAL 1 DAY) AS receival_date, 'BNI' AS bank, remarks,
    NULL, NULL, NULL, NULL, NULL, NULL, SUM(amount_received) AS amount_received
  FROM FILE_BANK_BNI WHERE remarks != 'Other' GROUP BY 1, 2, 3
),
BANK_SHOPEEPAY AS (
  SELECT DATE_SUB(DATE(receival_date), INTERVAL 1 DAY) AS receival_date, 'ShopeePay' AS bank, 'ShopeePay' AS remarks,
    NULL, NULL, NULL, NULL, NULL, NULL, SUM(amount_received) AS amount_received
  -- Replace description keywords with your actual ShopeePay remittance description patterns
  FROM FILE_BANK_BNI WHERE UPPER(description) LIKE '%YOUR_SHOPEEPAY_KEYWORD%'
  GROUP BY 1, 2, 3
),
BANK_OVO AS (
  SELECT DATE_SUB(DATE(receival_date), INTERVAL 1 DAY) AS receival_date, 'OVO' AS bank, 'OVO' AS remarks,
    NULL, NULL, NULL, NULL, NULL, NULL, SUM(amount_received) AS amount_received
  FROM FILE_BANK_BCA WHERE UPPER(description) LIKE '%YOUR_OVO_KEYWORD%'
  GROUP BY 1, 2, 3
),
BANK_INDODANA AS (
  SELECT DATE_SUB(DATE(receival_date), INTERVAL 1 DAY) AS receival_date, 'INDODANA' AS bank, 'INDODANA' AS remarks,
    NULL, NULL, NULL, NULL, NULL, NULL, SUM(amount_received) AS amount_received
  FROM FILE_BANK_BCA WHERE UPPER(description) LIKE '%YOUR_INDODANA_KEYWORD%'
  GROUP BY 1, 2, 3
),
BANK_BCA AS (
  SELECT DATE_SUB(DATE(receival_date), INTERVAL 1 DAY) AS receival_date, 'BCA' AS bank, 'BCA' AS remarks,
    NULL, NULL, NULL, NULL, NULL, NULL, SUM(amount_received) AS amount_received
  FROM FILE_BANK_BCA WHERE UPPER(description) LIKE '%YOUR_BCA_SETTLEMENT_KEYWORD%'
  GROUP BY 1, 2, 3
),
BANK_OTTOPAY AS (
  SELECT DATE_SUB(DATE(receival_date), INTERVAL 1 DAY) AS receival_date, 'OTTOPAY' AS bank, 'OTTOPAY' AS remarks,
    NULL, NULL, NULL, NULL, NULL, NULL, SUM(amount_received) AS amount_received
  FROM FILE_BANK_BCA WHERE UPPER(description) LIKE '%YOUR_OTTOPAY_KEYWORD%'
  GROUP BY 1, 2, 3
),
BANK_BTN AS (
  SELECT DATE_SUB(DATE(receival_date), INTERVAL 1 DAY) AS receival_date, 'BTN' AS bank, 'BTN' AS remarks,
    NULL, NULL, NULL, NULL, NULL, NULL, SUM(amount_received) AS amount_received
  FROM FILE_BANK_BTN WHERE UPPER(description) LIKE '%YOUR_BTN_SETTLE_KEYWORD%'
  GROUP BY 1, 2, 3
),
BANK_KREDIVO AS (
  -- Kredivo receives funds via two banks (adjust to your setup)
  SELECT DATE_SUB(DATE(receival_date), INTERVAL 1 DAY) AS receival_date, 'Kredivo' AS bank,
    CASE
      WHEN UPPER(description) LIKE '%YOUR_KREDIVO_FM_CODE%' THEN 'FM'
      WHEN UPPER(description) LIKE '%YOUR_KREDIVO_HERO_CODE%' THEN 'HERO'
      ELSE 'Other'
    END AS remarks,
    NULL, NULL, NULL, NULL, NULL, NULL, SUM(amount_received) AS amount_received
  FROM FILE_BANK_BCA WHERE UPPER(description) LIKE '%YOUR_KREDIVO_KEYWORD%'
  GROUP BY 1, 2, 3
),


-- =============================================================================
-- FINAL UNION — combine all three sources into one flat table
-- =============================================================================
FINAL AS (
  SELECT * FROM YTI_LINKAJA_MCD       UNION ALL SELECT * FROM ISSUER_LINKAJA_MCD
  UNION ALL SELECT * FROM YTI_LINKAJA_EXPANSION  UNION ALL SELECT * FROM ISSUER_LINKAJA_EXPANSION
  UNION ALL SELECT * FROM YTI_LINKAJA_MSME       UNION ALL SELECT * FROM ISSUER_LINKAJA_MSME
  UNION ALL SELECT * FROM BANK_LINKAJA
  UNION ALL SELECT * FROM YTI_BNI     UNION ALL SELECT * FROM ISSUER_BNI
  UNION ALL SELECT * FROM BANK_BNI    UNION ALL SELECT * FROM BNI_CROSS
  UNION ALL SELECT * FROM BANK_SHOPEEPAY
  UNION ALL SELECT * FROM YTI_SHOPEEPAY           UNION ALL SELECT * FROM ISSUER_SHOPEEPAY
  UNION ALL SELECT * FROM BANK_MANDIRI
  UNION ALL SELECT * FROM YTI_MANDIRI             UNION ALL SELECT * FROM ISSUER_MANDIRI
  UNION ALL SELECT * FROM MANDIRI_CROSS
  UNION ALL SELECT * FROM BANK_OVO
  UNION ALL SELECT * FROM YTI_OVO     UNION ALL SELECT * FROM ISSUER_OVO
  UNION ALL SELECT * FROM YTI_INDODANA            UNION ALL SELECT * FROM ISSUER_INDODANA
  UNION ALL SELECT * FROM YTI_KREDIVO             UNION ALL SELECT * FROM ISSUER_KREDIVO
  UNION ALL SELECT * FROM BANK_KREDIVO            UNION ALL SELECT * FROM BANK_INDODANA
  UNION ALL SELECT * FROM YTI_OTTOPAY             UNION ALL SELECT * FROM BANK_OTTOPAY
  UNION ALL SELECT * FROM ISSUER_OTTOPAY
  UNION ALL SELECT * FROM YTI_BCA     UNION ALL SELECT * FROM BANK_BCA
  UNION ALL SELECT * FROM ISSUER_BCA
  UNION ALL SELECT * FROM YTI_BTN     UNION ALL SELECT * FROM BANK_BTN
  UNION ALL SELECT * FROM ISSUER_BTN
)


-- =============================================================================
-- FINAL OUTPUT — aggregate to (date, issuer) granularity
-- Gap columns (A-B, C-D, G-F, G-E) are computed in the BI tool
-- =============================================================================
SELECT
  DATE(d.receival_date)              AS receival_date,
  d.issuer_name,
  d.bank,
  d.remark                           AS remarks,

  -- Column A: Production traffic
  SUM(ROUND(yti_traffic))            AS yti_traffic,
  -- Column C: Production transaction amount
  SUM(ROUND(yti_trx_amount))         AS yti_trx_amount,
  -- Column E: Production settlement amount (after MDR)
  SUM(ROUND(yti_settlement_amount))  AS yti_settlement_amount,

  -- Column B: Issuer traffic
  SUM(ROUND(issuer_traffic))         AS issuer_traffic,
  -- Column D: Issuer transaction amount
  SUM(ROUND(issuer_trx_amount))      AS issuer_trx_amount,
  -- Column F: Issuer settlement amount
  SUM(ROUND(issuer_settlement_amount)) AS issuer_settlement_amount,

  -- Column G: Actual money received in bank account
  SUM(ROUND(amount_received))        AS amount_received

FROM DATE_ARRAY d
LEFT JOIN FINAL f
  ON  d.receival_date = f.receival_date
  AND d.issuer_name   = CONCAT(f.bank, ' - ', f.remarks)

WHERE DATE(d.receival_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
  AND DATE(d.receival_date) <  CURRENT_DATE()

GROUP BY 1, 2, 3, 4
ORDER BY receival_date DESC

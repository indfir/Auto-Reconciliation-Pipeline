"""
airflow_dag.py
==============
Apache Airflow DAG — GCS to BigQuery loader for the reconciliation pipeline.

After the Google Apps Script formatter uploads standardized CSV files to GCS
(see issuer.js / bank.js), this DAG picks them up and loads each file into
its corresponding BigQuery table using GCSToBigQueryOperator.

Schedule : every hour  (@hourly)
Catchup  : disabled
Retries  : 1 per task, with a 5-minute delay

SETUP:
  1. Copy this file to your Airflow DAGs folder.
  2. Set BIGQUERY_PROJECT and BIGQUERY_DATASET to your own values.
  3. Set GCS_BUCKET to the same bucket used in config.example.js.
  4. Ensure the Airflow worker has a Google Cloud connection named
     "google_cloud_default" with BigQuery + GCS permissions.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# ─── Configuration ────────────────────────────────────────────────────────────
# Replace these with your actual values
BIGQUERY_PROJECT = "your-gcp-project-id"
BIGQUERY_DATASET = "your_dataset"
GCS_BUCKET       = "your-gcs-bucket"
GCP_CONN_ID      = "google_cloud_default"

# ─── Default arguments ────────────────────────────────────────────────────────
default_args = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# ─── DAG definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id          = "recon_pipeline_dag",
    default_args    = default_args,
    description     = "Load formatted issuer & bank CSV files from GCS into BigQuery",
    schedule_interval = "@hourly",
    catchup         = False,
    tags            = ["recon", "bigquery", "gcs"],
) as dag:

    # ── Helper: create a GCSToBigQueryOperator task ───────────────────────────
    def make_gcs_to_bq(
        task_id: str,
        gcs_prefix: str,
        bq_table: str,
        schema_fields: list,
        write_disposition: str = "WRITE_TRUNCATE",
        skip_leading_rows: int = 1,
    ) -> GCSToBigQueryOperator:
        """
        Creates a GCSToBigQueryOperator that loads all CSV files
        matching `gcs_prefix/*.csv` into the given BigQuery table.

        Args:
            task_id           : Airflow task ID (also used as display name in UI)
            gcs_prefix        : GCS path prefix under GCS_BUCKET (no leading slash)
            bq_table          : Target BigQuery table in the form "dataset.table"
            schema_fields     : List of BigQuery schema field dicts
            write_disposition : WRITE_TRUNCATE | WRITE_APPEND | WRITE_EMPTY
            skip_leading_rows : Number of header rows to skip (default 1)
        """
        return GCSToBigQueryOperator(
            task_id               = task_id,
            bucket                = GCS_BUCKET,
            source_objects        = [f"{gcs_prefix}/*.csv"],
            destination_project_dataset_table = f"{BIGQUERY_PROJECT}.{bq_table}",
            schema_fields         = schema_fields,
            field_delimiter       = ";",
            skip_leading_rows     = skip_leading_rows,
            write_disposition     = write_disposition,
            source_format         = "CSV",
            allow_quoted_newlines = True,
            gcp_conn_id           = GCP_CONN_ID,
        )

    # =========================================================================
    #  ISSUER REPORTS
    # =========================================================================

    # ── LinkAja MCD ──────────────────────────────────────────────────────────
    recon_linkaja_mcd = make_gcs_to_bq(
        task_id    = "recon_linkaja_mcd",
        gcs_prefix = "recon-project/issuer/linkaja/mcd",
        bq_table   = f"{BIGQUERY_DATASET}.recon_linkaja_mcd_report",
        schema_fields = [
            {"name": "transaction_date",  "type": "DATE",    "mode": "NULLABLE"},
            {"name": "transaction_time",  "type": "TIME",    "mode": "NULLABLE"},
            {"name": "ref_no",            "type": "STRING",  "mode": "NULLABLE"},
            {"name": "company_code",      "type": "STRING",  "mode": "NULLABLE"},
            {"name": "mid",               "type": "STRING",  "mode": "NULLABLE"},
            {"name": "merchant_name",     "type": "STRING",  "mode": "NULLABLE"},
            {"name": "transaction_amount","type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "mdr",               "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "net_amount",        "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "status",            "type": "STRING",  "mode": "NULLABLE"},
            {"name": "remark",            "type": "STRING",  "mode": "NULLABLE"},
        ],
    )

    # ── LinkAja Expansion ─────────────────────────────────────────────────────
    recon_linkaja_expansion = make_gcs_to_bq(
        task_id    = "recon_linkaja_expansion",
        gcs_prefix = "recon-project/issuer/linkaja/expansion",
        bq_table   = f"{BIGQUERY_DATASET}.recon_linkaja_expansion_report",
        schema_fields = [
            {"name": "transaction_date",  "type": "DATE",    "mode": "NULLABLE"},
            {"name": "ref_no",            "type": "STRING",  "mode": "NULLABLE"},
            {"name": "mid",               "type": "STRING",  "mode": "NULLABLE"},
            {"name": "merchant_name",     "type": "STRING",  "mode": "NULLABLE"},
            {"name": "transaction_amount","type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "net_amount",        "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "remark",            "type": "STRING",  "mode": "NULLABLE"},
            {"name": "chargebackoriginalbusinesstransactionid", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    # ── LinkAja MSME ──────────────────────────────────────────────────────────
    recon_linkaja_msme = make_gcs_to_bq(
        task_id    = "recon_linkaja_msme",
        gcs_prefix = "recon-project/issuer/linkaja/msme",
        bq_table   = f"{BIGQUERY_DATASET}.recon_linkaja_msme_report",
        schema_fields = [
            {"name": "transaction_date",  "type": "DATE",    "mode": "NULLABLE"},
            {"name": "transaction_time",  "type": "TIME",    "mode": "NULLABLE"},
            {"name": "ref_no",            "type": "STRING",  "mode": "NULLABLE"},
            {"name": "mid",               "type": "STRING",  "mode": "NULLABLE"},
            {"name": "merchant_name",     "type": "STRING",  "mode": "NULLABLE"},
            {"name": "transaction_amount","type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "net_amount",        "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "remark",            "type": "STRING",  "mode": "NULLABLE"},
            {"name": "chargebackoriginalbusinesstransactionid", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    # ── BCA Bank File ─────────────────────────────────────────────────────────
    recon_bca_bank_file = make_gcs_to_bq(
        task_id    = "recon_bca_bank_file",
        gcs_prefix = "recon-project/bank/bca/bank_file",
        bq_table   = f"{BIGQUERY_DATASET}.recon_bca_bank_file_report",
        schema_fields = [
            {"name": "transaction_date",    "type": "DATE",    "mode": "NULLABLE"},
            {"name": "description",         "type": "STRING",  "mode": "NULLABLE"},
            {"name": "branch",              "type": "STRING",  "mode": "NULLABLE"},
            {"name": "transaction_amount",  "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "type_trx",            "type": "STRING",  "mode": "NULLABLE"},
            {"name": "total_amount",        "type": "NUMERIC", "mode": "NULLABLE"},
        ],
    )

    # ── BNI Bank File ─────────────────────────────────────────────────────────
    recon_bni_bank_file = make_gcs_to_bq(
        task_id    = "recon_bni_bank_file",
        gcs_prefix = "recon-project/bank/bni/bank_file",
        bq_table   = f"{BIGQUERY_DATASET}.recon_bni_bank_file_report",
        schema_fields = [
            {"name": "post_date",    "type": "STRING",  "mode": "NULLABLE"},
            {"name": "value_date",   "type": "STRING",  "mode": "NULLABLE"},
            {"name": "branch",       "type": "STRING",  "mode": "NULLABLE"},
            {"name": "journal_no",   "type": "STRING",  "mode": "NULLABLE"},
            {"name": "description",  "type": "STRING",  "mode": "NULLABLE"},
            {"name": "debit",        "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "credit",       "type": "NUMERIC", "mode": "NULLABLE"},
        ],
    )

    # ── BNI Bank Report (Issuer) ──────────────────────────────────────────────
    recon_bni_bank_report = make_gcs_to_bq(
        task_id    = "recon_bni_bank_report",
        gcs_prefix = "recon-project/bank/bni/bank_report",
        bq_table   = f"{BIGQUERY_DATASET}.recon_bni_bank_report",
        schema_fields = [
            {"name": "bill_number",   "type": "STRING",  "mode": "NULLABLE"},
            {"name": "mid",           "type": "STRING",  "mode": "NULLABLE"},
            {"name": "nama_merchant", "type": "STRING",  "mode": "NULLABLE"},
            {"name": "nominal",       "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "net_amount",    "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "trx_datetime",  "type": "DATETIME","mode": "NULLABLE"},
            {"name": "status",        "type": "STRING",  "mode": "NULLABLE"},
        ],
    )

    # ── MDR Master ────────────────────────────────────────────────────────────
    recon_mdr_master = make_gcs_to_bq(
        task_id    = "recon_mdr_master",
        gcs_prefix = "recon-project/master/mdr",
        bq_table   = f"{BIGQUERY_DATASET}.recon_mdr_master",
        schema_fields = [
            {"name": "issuer",       "type": "STRING",  "mode": "NULLABLE"},
            {"name": "merchant_type","type": "STRING",  "mode": "NULLABLE"},
            {"name": "mdr_rate",     "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "effective_date","type": "DATE",   "mode": "NULLABLE"},
        ],
    )

    # ── Indodana ──────────────────────────────────────────────────────────────
    recon_indodana = make_gcs_to_bq(
        task_id    = "recon_indodana",
        gcs_prefix = "recon-project/issuer/indodana",
        bq_table   = f"{BIGQUERY_DATASET}.recon_indodana_report",
        schema_fields = [
            {"name": "NO",               "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "MERCHANT_NAME",    "type": "STRING",  "mode": "NULLABLE"},
            {"name": "TRANSACTION_DATE", "type": "DATETIME","mode": "NULLABLE"},
            {"name": "TRANSIDMERCHANT",  "type": "STRING",  "mode": "NULLABLE"},
            {"name": "CUSTOMER_NAME",    "type": "STRING",  "mode": "NULLABLE"},
            {"name": "AMOUNT",           "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "FEE",              "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "TAX",              "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "MERCHANT_SUPPORT", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "PAY_TO_MERCHANT",  "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "PAY_OUT_DATE",     "type": "DATETIME","mode": "NULLABLE"},
            {"name": "TRANSACTION_TYPE", "type": "STRING",  "mode": "NULLABLE"},
            {"name": "TENURE",           "type": "STRING",  "mode": "NULLABLE"},
        ],
    )

    # ── Kredivo ───────────────────────────────────────────────────────────────
    recon_kredivo = make_gcs_to_bq(
        task_id    = "recon_kredivo",
        gcs_prefix = "recon-project/issuer/kredivo",
        bq_table   = f"{BIGQUERY_DATASET}.recon_kredivo_report",
        schema_fields = [
            {"name": "Name",               "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Transaction_Date",   "type": "DATETIME","mode": "NULLABLE"},
            {"name": "User_ID",            "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Settlement_Date",    "type": "DATE",    "mode": "NULLABLE"},
            {"name": "Cancellation_Date",  "type": "DATE",    "mode": "NULLABLE"},
            {"name": "Order_ID",           "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Transaction_ID",     "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Amount",             "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "Type",               "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Status",             "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Source",             "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Store_ID",           "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Store_Name",         "type": "STRING",  "mode": "NULLABLE"},
        ],
    )

    # ── OVO ───────────────────────────────────────────────────────────────────
    recon_ovo = make_gcs_to_bq(
        task_id    = "recon_ovo",
        gcs_prefix = "recon-project/issuer/ovo",
        bq_table   = f"{BIGQUERY_DATASET}.recon_ovo_report",
        schema_fields = [
            {"name": "merchantinvoice",    "type": "STRING",  "mode": "NULLABLE"},
            {"name": "transactiondate",    "type": "DATETIME","mode": "NULLABLE"},
            {"name": "transactiontype",    "type": "STRING",  "mode": "NULLABLE"},
            {"name": "transactionamount",  "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "nettsettlement",     "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "status",             "type": "STRING",  "mode": "NULLABLE"},
        ],
    )

    # ── ShopeePay ─────────────────────────────────────────────────────────────
    recon_shopeepay = make_gcs_to_bq(
        task_id    = "recon_shopeepay",
        gcs_prefix = "recon-project/issuer/shopeepay",
        bq_table   = f"{BIGQUERY_DATASET}.recon_shopeepay_report",
        schema_fields = [
            {"name": "transaction_id",     "type": "STRING",  "mode": "NULLABLE"},
            {"name": "transaction_type",   "type": "STRING",  "mode": "NULLABLE"},
            {"name": "transaction_amount", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "settlement_amount",  "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "create_time",        "type": "DATETIME","mode": "NULLABLE"},
            {"name": "status",             "type": "STRING",  "mode": "NULLABLE"},
        ],
    )

    # ── Mandiri Bank Report (Issuer) ──────────────────────────────────────────
    recon_mandiri_bank_report = make_gcs_to_bq(
        task_id    = "recon_mandiri_bank_report",
        gcs_prefix = "recon-project/bank/mandiri/bank_report",
        bq_table   = f"{BIGQUERY_DATASET}.recon_mandiri_bank_report",
        schema_fields = [
            {"name": "NMID",        "type": "STRING",  "mode": "NULLABLE"},
            {"name": "MID",         "type": "STRING",  "mode": "NULLABLE"},
            {"name": "trading_name","type": "STRING",  "mode": "NULLABLE"},
            {"name": "TRXDATE",     "type": "DATE",    "mode": "NULLABLE"},
            {"name": "TRXTIME",     "type": "TIME",    "mode": "NULLABLE"},
            {"name": "reff_id",     "type": "STRING",  "mode": "NULLABLE"},
            {"name": "amount",      "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "net_amount",  "type": "NUMERIC", "mode": "NULLABLE"},
        ],
    )

    # ── Mandiri Bank File ─────────────────────────────────────────────────────
    recon_mandiri_bank_file = make_gcs_to_bq(
        task_id    = "recon_mandiri_bank_file",
        gcs_prefix = "recon-project/bank/mandiri/bank_file",
        bq_table   = f"{BIGQUERY_DATASET}.recon_mandiri_bank_file_report",
        schema_fields = [
            {"name": "AccountNo",      "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "Ccy",            "type": "STRING",  "mode": "NULLABLE"},
            {"name": "postdate",       "type": "DATETIME","mode": "NULLABLE"},
            {"name": "remarks",        "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Additional_Desc","type": "STRING",  "mode": "NULLABLE"},
            {"name": "credit_amount",  "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "debit_amount",   "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "close_balance",  "type": "NUMERIC", "mode": "NULLABLE"},
        ],
    )

    # ── BCA Issuer Bank Report ────────────────────────────────────────────────
    recon_bca_issuer_bank_report = make_gcs_to_bq(
        task_id    = "recon_bca_issuer_bank_report",
        gcs_prefix = "recon-project/issuer/bca",
        bq_table   = f"{BIGQUERY_DATASET}.recon_issuer_bca_bank_file_report",
        schema_fields = [
            {"name": "reference_no",  "type": "STRING",  "mode": "NULLABLE"},
            {"name": "payment_date",  "type": "DATETIME","mode": "NULLABLE"},
            {"name": "base_amount",   "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "nett",          "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "status",        "type": "STRING",  "mode": "NULLABLE"},
        ],
    )

    # ── BTN Bank Mutation File ────────────────────────────────────────────────
    recon_btn_bank_mutation_file = make_gcs_to_bq(
        task_id    = "recon_btn_bank_mutation_file",
        gcs_prefix = "recon-project/bank/btn/mutation_file",
        bq_table   = f"{BIGQUERY_DATASET}.recon_btn_bank_file_mutation",
        schema_fields = [
            {"name": "post_date",      "type": "DATETIME","mode": "NULLABLE"},
            {"name": "post_time",      "type": "STRING",  "mode": "NULLABLE"},
            {"name": "eff_date",       "type": "DATETIME","mode": "NULLABLE"},
            {"name": "eff_time",       "type": "STRING",  "mode": "NULLABLE"},
            {"name": "description",    "type": "STRING",  "mode": "NULLABLE"},
            {"name": "amount_debit",   "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "amount_credit",  "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "balance",        "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "reference_no",   "type": "STRING",  "mode": "NULLABLE"},
        ],
    )

    # ── BTN Issuer Bank Report ────────────────────────────────────────────────
    recon_btn_issuer_bank_report = make_gcs_to_bq(
        task_id    = "recon_btn_issuer_bank_report",
        gcs_prefix = "recon-project/issuer/btn",
        bq_table   = f"{BIGQUERY_DATASET}.recon_issuer_btn_bank_file_report",
        schema_fields = [
            {"name": "transaction_date",              "type": "DATE",    "mode": "NULLABLE"},
            {"name": "transaction_time",              "type": "TIME",    "mode": "NULLABLE"},
            {"name": "retrieval_reference_number",    "type": "STRING",  "mode": "NULLABLE"},
            {"name": "merchant_id",                   "type": "STRING",  "mode": "NULLABLE"},
            {"name": "amount",                        "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "mdr",                           "type": "STRING",  "mode": "NULLABLE"},
            {"name": "status",                        "type": "STRING",  "mode": "NULLABLE"},
        ],
    )

    # ── OttoPay Issuer Bank Report ────────────────────────────────────────────
    recon_ottopay_issuer_bank_report = make_gcs_to_bq(
        task_id    = "recon_ottopay_issuer_bank_report",
        gcs_prefix = "recon-project/issuer/ottopay",
        bq_table   = f"{BIGQUERY_DATASET}.recon_issuer_ottopay_bank_file_report",
        schema_fields = [
            {"name": "issuer_rrn",       "type": "STRING",  "mode": "NULLABLE"},
            {"name": "transaction_time", "type": "DATETIME","mode": "NULLABLE"},
            {"name": "gross_amount",     "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "nett_amount",      "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "payment_status",   "type": "STRING",  "mode": "NULLABLE"},
        ],
    )

    # ── OttoPay Dashboard Issuer Bank Report ──────────────────────────────────
    recon_ottopay_dashboard_issuer_bank_report = make_gcs_to_bq(
        task_id    = "recon_ottopay_dashboard_issuer_bank_report",
        gcs_prefix = "recon-project/issuer/ottopay/dashboard",
        bq_table   = f"{BIGQUERY_DATASET}.recon_ottopay_dashboard_report",
        schema_fields = [
            {"name": "Order_ID",            "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Invoice_Number",      "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Payment_Method",      "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Transaction_Date",    "type": "DATETIME","mode": "NULLABLE"},
            {"name": "Store_Name",          "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Status_Transaction",  "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Status_Invoice",      "type": "STRING",  "mode": "NULLABLE"},
            {"name": "Gross_Amount",        "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "Reference_Number",    "type": "STRING",  "mode": "NULLABLE"},
        ],
    )

    # =========================================================================
    #  TASK ORDERING
    #  All 18 tasks are independent (no upstream dependencies between them).
    #  They run in parallel as soon as the DAG triggers.
    # =========================================================================
    # To add task dependencies, use: task_a >> task_b
    # Example: recon_mdr_master >> recon_linkaja_mcd

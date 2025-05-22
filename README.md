# Banking Data Pipeline Project

This project implements a data pipeline for processing banking data using Google Cloud Platform (GCP) services. It ingests raw CSV data, processes it through several layers (Bronze, Silver, Gold) in BigQuery, and provides analytical queries for data products.

## Project Structure

The pipeline consists of the following components:

*   **`main.py`**: A Python Google Cloud Function triggered by CSV file uploads to a GCS landing bucket. It converts the CSV data to Parquet format and stores it in the Bronze layer GCS bucket.
*   **`requirements.txt`**: Specifies the Python dependencies for the `main.py` Cloud Function.
*   **SQL Scripts**:
    *   **`bronze_layer_setup.sql`**: Contains DDL statements to create the schema and external tables in BigQuery for the Bronze layer. These tables point to Parquet files in GCS.
    *   **`silver_layer_transformations.sql`**: Contains DDL and DML statements to create tables in the Silver layer. These tables represent cleaned, typed, and transformed data from the Bronze layer.
    *   **`gold_layer_star_schema_views.sql`**: Contains DDL statements to create views in the Gold layer, forming a star schema for analytical purposes. These views are built on top of the Silver layer tables.
    *   **`data_product_queries.sql`**: Includes sample SQL queries that can be run against the Gold layer to generate data products or insights.

## Data Pipeline Layers

1.  **Landing Zone (GCS)**:
    *   Raw data is expected to be uploaded in CSV format to a specified GCS bucket (e.g., `gs://your-landing-bucket/uploads/`).
    *   Supported CSV files are `customers.csv`, `products.csv`, and `transactions.csv`.

2.  **Bronze Layer (GCS & BigQuery)**:
    *   The `csv_to_bronze_parquet_ingestor` Cloud Function (`main.py`) automatically processes new CSV files from the landing zone.
    *   It converts CSVs to Parquet format.
    *   Stores Parquet files in a GCS bucket (e.g., `gs://your-gcp-project-id-banking-data-bronze/banking/{table_name}/load_date={YYYYMMDD}/`).
    *   `bronze_layer_setup.sql` defines external tables in BigQuery that point to these Parquet files, making the raw data queryable.

3.  **Silver Layer (BigQuery)**:
    *   `silver_layer_transformations.sql` is run to transform data from the Bronze layer.
    *   This involves data type casting, cleaning (e.g., `TRIM`), and structuring the data into normalized tables (e.g., `silver_layer.customers`, `silver_layer.products`, `silver_layer.transactions`).
    *   Technical metadata columns (e.g., `ingestion_timestamp`, `pipeline_version`) are added.

4.  **Gold Layer (BigQuery)**:
    *   `gold_layer_star_schema_views.sql` is run to create a star schema on top of the Silver layer.
    *   This typically involves creating dimension views (e.g., `gold_layer.dim_customers`, `gold_layer.dim_products`, `gold_layer.dim_date`) and a fact view (e.g., `gold_layer.fact_transactions`).
    *   This layer is optimized for analytical queries.

5.  **Data Products**:
    *   `data_product_queries.sql` provides examples of how to query the Gold layer to extract meaningful insights, such as top expenses or investment trends.

## Prerequisites

*   Google Cloud Platform (GCP) project.
*   Google Cloud Storage (GCS) buckets for:
    *   Landing CSV files.
    *   Storing Bronze layer Parquet files.
*   BigQuery API enabled.
*   Google Cloud Functions API enabled.
*   Python 3.x environment for deploying the Cloud Function (or use Google Cloud Shell).
*   `gcloud` CLI configured.

## Setup and Execution

1.  **Configure Placeholders**:
    *   In `main.py`, update `TARGET_BRONZE_BUCKET` with your GCS bucket name for the bronze layer.
    *   In `bronze_layer_setup.sql`, update the `uris` in the `CREATE EXTERNAL TABLE` statements to point to your Bronze GCS bucket path (e.g., `gs://your-gcp-project-id-banking-data-bronze/banking/customers/*`).

2.  **Deploy Cloud Function (`main.py`)**:
    *   Navigate to the directory containing `main.py` and `requirements.txt`.
    *   Deploy the function using `gcloud` (ensure you specify the correct trigger bucket -- your landing zone):
        ```bash
        gcloud functions deploy csv_to_bronze_parquet_ingestor         --runtime python39 \ # Or your preferred Python runtime
        --trigger-resource YOUR_LANDING_GCS_BUCKET_NAME         --trigger-event google.storage.object.finalize         --entry-point csv_to_bronze_parquet_ingestor         --region YOUR_GCP_REGION
        ```
    *   Ensure the Cloud Function's service account has permissions to read from the landing bucket and write to the bronze bucket.

3.  **Apply BigQuery SQL DDLs**:
    *   Execute the SQL scripts in BigQuery in the following order:
        1.  `bronze_layer_setup.sql` (after ensuring Parquet files are expected or present from function runs)
        2.  `silver_layer_transformations.sql`
        3.  `gold_layer_star_schema_views.sql`

4.  **Ingest Data**:
    *   Upload CSV files (e.g., `customers.csv`, `products.csv`, `transactions.csv`) to the `uploads/` directory in your configured GCS landing bucket.
    *   The Cloud Function will automatically process them, creating Parquet files in the Bronze GCS bucket.

5.  **Run Data Product Queries**:
    *   Execute the queries in `data_product_queries.sql` against your BigQuery Gold layer views.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

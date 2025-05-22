import functions_framework
from google.cloud import storage
import pandas as pd
import pyarrow
from datetime import datetime
import os

# Configuration (replace with your actual project ID or environment variables)
TARGET_BRONZE_BUCKET = "placeholder-bronze-bucket" 
# For local testing, you might need to set GOOGLE_APPLICATION_CREDENTIALS
# storage_client = storage.Client.from_service_account_json("path/to/your/key.json")
storage_client = storage.Client()

@functions_framework.cloud_event
def csv_to_bronze_parquet_ingestor(cloud_event):
    """
    Cloud Function to ingest CSV data from a landing GCS bucket,
    transform it to Parquet, and store it in a bronze GCS bucket.
    """
    data = cloud_event.data
    source_bucket_name = data["bucket"]
    source_file_name = data["name"]

    print(f"Received event for file: {source_file_name} in bucket: {source_bucket_name}")

    # Ensure the function only processes files in the 'uploads/' prefix
    if not source_file_name.startswith("uploads/"):
        print(f"File {source_file_name} is not in 'uploads/' prefix. Skipping.")
        return

    # Determine table name from the input CSV filename
    # e.g., uploads/customers.csv -> customers
    base_name = os.path.basename(source_file_name)
    table_name, file_ext = os.path.splitext(base_name)

    if file_ext.lower() != ".csv":
        print(f"File {source_file_name} is not a CSV file. Skipping.")
        return

    print(f"Processing CSV file: {source_file_name}")
    print(f"Determined table name: {table_name}")

    try:
        # Read CSV file from the landing bucket
        source_bucket = storage_client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_file_name)

        if not source_blob.exists():
            print(f"Error: File {source_file_name} not found in bucket {source_bucket_name}.")
            # Consider raising an exception or returning a specific error response
            return

        print(f"Reading CSV from gs://{source_bucket_name}/{source_file_name}")
        df = pd.read_csv(source_blob.open("r"))
        print(f"Successfully read CSV into DataFrame. Shape: {df.shape}")

        # Convert DataFrame to Parquet format
        print("Converting DataFrame to Parquet format...")
        parquet_bytes = df.to_parquet(engine='pyarrow')
        print("Conversion to Parquet successful.")

        # Construct the output path
        load_date_str = datetime.utcnow().strftime("%Y%m%d")
        output_filename_parquet = f"{table_name}.parquet"
        output_path = f"banking/{table_name}/load_date={load_date_str}/{output_filename_parquet}"

        print(f"Constructed output path: {output_path}")

        # Upload Parquet file to the bronze GCS bucket
        bronze_bucket = storage_client.bucket(TARGET_BRONZE_BUCKET)
        target_blob = bronze_bucket.blob(output_path)

        print(f"Uploading Parquet file to gs://{TARGET_BRONZE_BUCKET}/{output_path}...")
        target_blob.upload_from_string(parquet_bytes, content_type='application/octet-stream')
        print(f"Successfully uploaded Parquet file to gs://{TARGET_BRONZE_BUCKET}/{output_path}")

        print(f"Successfully processed {source_file_name} to {output_path}")

    except pd.errors.EmptyDataError:
        print(f"Error: CSV file {source_file_name} is empty.")
    except pd.errors.ParserError:
        print(f"Error: Could not parse CSV file {source_file_name}. It might be malformed.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        # Optionally, re-raise the exception if you want the function invocation to be marked as failed
        # raise

    return # Explicitly return None or a success message/status if required by framework conventions

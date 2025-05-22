# Sample Data Generation for Banking Data Pipeline (`feat/generate-sample-data` branch)

This branch (`feat/generate-sample-data`) provides tools and/or scripts to generate sample CSV data for the Banking Data Pipeline project. This is useful for testing the pipeline's functionality, development purposes, or creating demonstrations without using real sensitive data.

## Purpose

The main goal of this branch is to enable users to quickly populate the data pipeline's landing zone with realistic-looking sample data for `customers`, `products`, and `transactions`.

## How it Works (Assumed Structure)

*(Please update this section based on the actual implementation in this branch. If this branch contains specific scripts, list them here, e.g., `scripts/generate_customers.py`)*

It is assumed that this branch includes:

*   **Scripts**: Python scripts (e.g., `generate_customers.py`, `generate_products.py`, `generate_transactions.py`) or a unified script (e.g., `generate_all_sample_data.py`).
*   **Configuration**: Possibly configuration files (e.g., YAML, JSON) to control the volume, format, or characteristics of the generated data.
*   **Output**: The scripts will output CSV files (`customers.csv`, `products.csv`, `transactions.csv`) that conform to the schemas expected by the main data pipeline.

## Prerequisites

*   Python 3.x environment.
*   Any libraries specified in a local `requirements-sample-data.txt` (if applicable, create one if needed).
*   Understanding of the main pipeline's data schema (see the main project `README.md`).

## Usage Instructions (Example)

*(Please update this section with actual commands, script names, and steps relevant to this branch.)*

1.  **Checkout the branch**:
    ```bash
    git checkout feat/generate-sample-data
    ```

2.  **Set up Environment**:
    ```bash
    # If there's a specific requirements file for data generation in this branch
    # pip install -r requirements-sample-data.txt 
    ```

3.  **Run Generation Scripts** (Update with actual script names and commands):
    *   Example if there are individual scripts:
        ```bash
        # python scripts/generate_customers.py --count 100 --output_path output/customers.csv
        # python scripts/generate_products.py --count 20 --output_path output/products.csv
        # python scripts/generate_transactions.py --input_customers output/customers.csv --input_products output/products.csv --count 1000 --output_path output/transactions.csv
        ```
    *   Example if there's a unified script:
        ```bash
        # python scripts/generate_all_sample_data.py --output_dir output/
        ```

4.  **Upload to Landing Zone**:
    *   Once the CSV files are generated (e.g., in an `output/` directory), upload them to the GCS landing bucket that your `csv_to_bronze_parquet_ingestor` Cloud Function monitors (e.g., `gs://your-landing-bucket/uploads/`).

    ```bash
    # gsutil cp output/customers.csv gs://your-landing-bucket/uploads/customers.csv
    # gsutil cp output/products.csv gs://your-landing-bucket/uploads/products.csv
    # gsutil cp output/transactions.csv gs://your-landing-bucket/uploads/transactions.csv
    ```

## Customization

*   Explain how users can customize the data generation (e.g., number of records, date ranges, specific data patterns).
*   Mention any configuration files or command-line arguments available for the scripts in this branch.

## Note

*   This sample data is for development and testing only.
*   Ensure the generated data aligns with the schema expectations of the Bronze layer to avoid issues in the downstream pipeline processes.
*   Refer to the main project `README.md` (in the main branch) for details on the overall data pipeline.
*   **After generating data, remember to switch back to the main branch if you intend to run the full pipeline with it.**

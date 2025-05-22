-- SQL DDL and Transformation Scripts for BigQuery Silver Layer

-- 1. Create Silver Layer Dataset
CREATE SCHEMA IF NOT EXISTS silver_layer
  OPTIONS (
    description = 'Dataset for silver layer (cleaned, typed) banking data',
    location = 'US' -- Ensure this location is consistent with your other datasets like bronze_layer
  );

-- 2. Define Silver Table Schemas and Create Table As Select (CTAS) Statements

-- Customers Table
-- This CTAS statement creates the silver_layer.customers table by selecting and transforming data
-- from the bronze_layer.ext_customers external table.
-- NOTE: For simplicity, this query processes all data available in the external table.
-- In a production scenario, this would typically be an incremental load,
-- filtering on specific load_date values from the bronze layer (e.g., WHERE load_date = 'YYYYMMDD').
CREATE OR REPLACE TABLE silver_layer.customers AS
SELECT
  SAFE_CAST(TRIM(customer_id) AS STRING) AS customer_id,
  SAFE_CAST(TRIM(first_name) AS STRING) AS first_name,
  SAFE_CAST(TRIM(last_name) AS STRING) AS last_name,
  SAFE_CAST(TRIM(email) AS STRING) AS email,
  SAFE_CAST(TRIM(phone_number) AS STRING) AS phone_number,
  SAFE_CAST(TRIM(address_line_1) AS STRING) AS address_line_1,
  SAFE_CAST(TRIM(address_line_2) AS STRING) AS address_line_2,
  SAFE_CAST(TRIM(city) AS STRING) AS city,
  SAFE_CAST(TRIM(state) AS STRING) AS state,
  SAFE_CAST(TRIM(zip_code) AS STRING) AS zip_code,
  SAFE_CAST(TRIM(country) AS STRING) AS country,
  SAFE_CAST(registration_date AS DATE) AS registration_date, -- Assuming YYYY-MM-DD format from source
  SAFE_CAST(TRIM(customer_segment) AS STRING) AS customer_segment,
  -- Technical Columns
  'Landing_CSV' AS source_system,
  CURRENT_TIMESTAMP() AS ingestion_timestamp,
  SAFE_CAST(NULL AS NUMERIC) AS data_quality_score, -- Defaulting to NULL, can be 1.0
  'v1.0' AS pipeline_version
FROM
  bronze_layer.ext_customers;

-- Products Table
-- This CTAS statement creates the silver_layer.products table by selecting and transforming data
-- from the bronze_layer.ext_products external table.
-- NOTE: For simplicity, this query processes all data available in the external table.
-- Incremental loading based on load_date is recommended for production.
CREATE OR REPLACE TABLE silver_layer.products AS
SELECT
  SAFE_CAST(TRIM(product_id) AS STRING) AS product_id,
  SAFE_CAST(TRIM(product_name) AS STRING) AS product_name,
  SAFE_CAST(TRIM(product_category) AS STRING) AS product_category,
  SAFE_CAST(TRIM(product_type) AS STRING) AS product_type,
  SAFE_CAST(interest_rate AS NUMERIC) AS interest_rate,
  SAFE_CAST(TRIM(fee_details) AS STRING) AS fee_details,
  SAFE_CAST(creation_date AS DATE) AS creation_date, -- Assuming YYYY-MM-DD format
  SAFE_CAST(last_updated_date AS DATE) AS last_updated_date, -- Assuming YYYY-MM-DD format
  SAFE_CAST(TRIM(status) AS STRING) AS status,
  -- Technical Columns
  'Landing_CSV' AS source_system,
  CURRENT_TIMESTAMP() AS ingestion_timestamp,
  SAFE_CAST(NULL AS NUMERIC) AS data_quality_score,
  'v1.0' AS pipeline_version
FROM
  bronze_layer.ext_products;

-- Transactions Table
-- This CTAS statement creates the silver_layer.transactions table by selecting and transforming data
-- from the bronze_layer.ext_transactions external table.
-- It includes logic to combine date and time fields into a single timestamp.
-- NOTE: For simplicity, this query processes all data available in the external table.
-- Incremental loading based on load_date is recommended for production.
CREATE OR REPLACE TABLE silver_layer.transactions AS
SELECT
  SAFE_CAST(TRIM(transaction_id) AS STRING) AS transaction_id,
  SAFE_CAST(TRIM(customer_id) AS STRING) AS customer_id,
  SAFE_CAST(TRIM(account_id) AS STRING) AS account_id,
  SAFE_CAST(TRIM(product_id) AS STRING) AS product_id,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CONCAT(TRIM(transaction_date), ' ', TRIM(transaction_time))) AS transaction_datetime,
  SAFE_CAST(TRIM(transaction_type) AS STRING) AS transaction_type,
  SAFE_CAST(transaction_amount AS NUMERIC) AS transaction_amount,
  SAFE_CAST(TRIM(currency) AS STRING) AS currency,
  SAFE_CAST(TRIM(merchant_name) AS STRING) AS merchant_name,
  SAFE_CAST(TRIM(transaction_description) AS STRING) AS transaction_description,
  SAFE_CAST(TRIM(payment_method) AS STRING) AS payment_method,
  SAFE_CAST(TRIM(transaction_status) AS STRING) AS transaction_status,
  SAFE_CAST(TRIM(location_city) AS STRING) AS location_city,
  SAFE_CAST(TRIM(location_country) AS STRING) AS location_country,
  -- Technical Columns
  'Landing_CSV' AS source_system,
  CURRENT_TIMESTAMP() AS ingestion_timestamp,
  SAFE_CAST(NULL AS NUMERIC) AS data_quality_score,
  'v1.0' AS pipeline_version
FROM
  bronze_layer.ext_transactions;

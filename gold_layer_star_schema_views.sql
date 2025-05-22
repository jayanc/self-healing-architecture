-- SQL DDL Scripts for BigQuery Gold Layer (Star Schema Views)

-- 1. Create Gold Layer Dataset
CREATE SCHEMA IF NOT EXISTS gold_layer
  OPTIONS (
    description = 'Dataset for gold layer (star schema, analytical) banking data',
    location = 'US' -- Ensure this location is consistent with your other datasets
  );

-- 2. Create View DDLs for Dimensions and Fact Table

-- Dimension View for Customers
CREATE OR REPLACE VIEW gold_layer.dim_customers AS
SELECT
  customer_id AS customer_key, -- Using customer_id as the business key and surrogate key for simplicity
  customer_id,
  CONCAT(first_name, ' ', last_name) AS full_name,
  email,
  phone_number,
  CONCAT(
    address_line_1,
    ', ',
    COALESCE(address_line_2, ''),
    CASE WHEN COALESCE(address_line_2, '') != '' THEN ', ' ELSE '' END, -- Add comma only if address_line_2 is not empty
    city,
    ', ',
    state,
    ', ',
    zip_code,
    ', ',
    country
  ) AS address,
  city,
  state,
  country,
  customer_segment,
  registration_date,
  ingestion_timestamp AS silver_ingestion_timestamp,
  pipeline_version
FROM
  silver_layer.customers;

-- Dimension View for Products
CREATE OR REPLACE VIEW gold_layer.dim_products AS
SELECT
  product_id AS product_key, -- Using product_id as the business key and surrogate key
  product_id,
  product_name,
  product_category,
  product_type,
  status,
  ingestion_timestamp AS silver_ingestion_timestamp,
  pipeline_version
FROM
  silver_layer.products;

-- Dimension View for Date
-- This view derives date attributes from the transaction_datetime column of the silver_layer.transactions table.
CREATE OR REPLACE VIEW gold_layer.dim_date AS
WITH DistinctDates AS (
  SELECT DISTINCT DATE(transaction_datetime) AS full_date
  FROM silver_layer.transactions
  WHERE transaction_datetime IS NOT NULL -- Ensure we only process valid dates
)
SELECT
  CAST(FORMAT_DATE('%Y%m%d', full_date) AS INT64) AS date_key,
  full_date,
  EXTRACT(YEAR FROM full_date) AS year,
  EXTRACT(QUARTER FROM full_date) AS quarter,
  EXTRACT(MONTH FROM full_date) AS month,
  EXTRACT(DAY FROM full_date) AS day_of_month,
  EXTRACT(DAYOFWEEK FROM full_date) AS day_of_week, -- 1 (Sunday) to 7 (Saturday)
  FORMAT_DATE('%B', full_date) AS month_name,
  EXTRACT(ISOWEEK FROM full_date) AS week_of_year, -- ISO 8601 week number
  CASE WHEN EXTRACT(DAYOFWEEK FROM full_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM
  DistinctDates;

-- Fact View for Transactions
CREATE OR REPLACE VIEW gold_layer.fact_transactions AS
SELECT
  s_trans.transaction_id AS transaction_key,
  dc.customer_key,
  dp.product_key,
  dd.date_key,
  s_trans.account_id,
  s_trans.transaction_datetime,
  s_trans.transaction_type,
  s_trans.transaction_amount,
  s_trans.currency,
  s_trans.merchant_name,
  s_trans.transaction_description,
  s_trans.payment_method,
  s_trans.transaction_status,
  s_trans.ingestion_timestamp AS silver_ingestion_timestamp,
  s_trans.pipeline_version
FROM
  silver_layer.transactions AS s_trans
JOIN
  gold_layer.dim_customers AS dc ON s_trans.customer_id = dc.customer_id
JOIN
  gold_layer.dim_products AS dp ON s_trans.product_id = dp.product_id
JOIN
  gold_layer.dim_date AS dd ON DATE(s_trans.transaction_datetime) = dd.full_date;

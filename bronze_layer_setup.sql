-- 1. Create Dataset DDL
CREATE SCHEMA IF NOT EXISTS bronze_layer
  OPTIONS (
    description = 'Dataset for bronze layer (raw) banking data',
    location = 'US' -- Specify your desired default location
  );

-- 2. Define Schemas and Create External Table DDLs

-- External Table for Customers
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_layer.ext_customers (
  customer_id STRING,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone_number STRING,
  address_line_1 STRING,
  address_line_2 STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  country STRING,
  registration_date STRING,
  customer_segment STRING
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your-gcp-project-id-banking-data-bronze/banking/customers/*'], -- Wildcard to include all parquet files under the base path
  hive_partition_uri_prefix = 'gs://your-gcp-project-id-banking-data-bronze/banking/customers/',
  partition_by = [('load_date', 'STRING')]
);

-- External Table for Products
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_layer.ext_products (
  product_id STRING,
  product_name STRING,
  product_category STRING,
  product_type STRING,
  interest_rate STRING,
  fee_details STRING,
  creation_date STRING,
  last_updated_date STRING,
  status STRING
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your-gcp-project-id-banking-data-bronze/banking/products/*'],
  hive_partition_uri_prefix = 'gs://your-gcp-project-id-banking-data-bronze/banking/products/',
  partition_by = [('load_date', 'STRING')]
);

-- External Table for Transactions
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_layer.ext_transactions (
  transaction_id STRING,
  customer_id STRING,
  account_id STRING,
  product_id STRING,
  transaction_date STRING,
  transaction_time STRING,
  transaction_type STRING,
  transaction_amount STRING,
  currency STRING,
  merchant_name STRING,
  transaction_description STRING,
  payment_method STRING,
  transaction_status STRING,
  location_city STRING,
  location_country STRING
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your-gcp-project-id-banking-data-bronze/banking/transactions/*'],
  hive_partition_uri_prefix = 'gs://your-gcp-project-id-banking-data-bronze/banking/transactions/',
  partition_by = [('load_date', 'STRING')]
);

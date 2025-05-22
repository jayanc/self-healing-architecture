-- SQL Queries for Data Products from the Gold Layer

-- Query 1: Top 10 Expenses This Month
-- This query identifies the top 10 categories of expenses for the current calendar month.
-- Expenses are defined by transaction types ('DEBIT', 'PAYMENT', 'TRANSFER_OUT', 'FEE')
-- and product categories that are not 'Investment', 'Savings', or 'Securities'.
WITH MonthlyExpenses AS (
  SELECT
    ft.merchant_name,
    dp.product_category,
    SUM(ft.transaction_amount) AS total_expense_amount -- Per prompt, not ABS for expenses
  FROM
    gold_layer.fact_transactions AS ft
  JOIN
    gold_layer.dim_date AS dd
    ON ft.date_key = dd.date_key
  JOIN
    gold_layer.dim_products AS dp
    ON ft.product_key = dp.product_key
  WHERE
    dd.year = EXTRACT(YEAR FROM CURRENT_DATE())
    AND dd.month = EXTRACT(MONTH FROM CURRENT_DATE())
    AND ft.transaction_type IN ('DEBIT', 'PAYMENT', 'TRANSFER_OUT', 'FEE')
    AND dp.product_category IS NOT NULL -- Updated based on subtask
    AND dp.product_category NOT IN ('Investment', 'Savings', 'Securities') -- Updated based on subtask
  GROUP BY
    ft.merchant_name,
    dp.product_category
)
SELECT
  COALESCE(merchant_name, 'Unknown Merchant') AS expense_source,
  product_category,
  total_expense_amount
FROM
  MonthlyExpenses
ORDER BY
  total_expense_amount DESC
LIMIT 10;

-- Query 2: Top 10 Investments This Month
-- This query identifies the top 10 investment transactions for the current calendar month.
-- Investments are defined by specific transaction types and product categories.
-- We sum ABS(transaction_amount) for investment-categorized products.
WITH MonthlyInvestments AS (
  SELECT
    dp.product_name,
    dp.product_category,
    SUM(ABS(ft.transaction_amount)) AS total_investment_volume -- Updated to ABS and renamed for clarity
  FROM
    gold_layer.fact_transactions AS ft
  JOIN
    gold_layer.dim_date AS dd
    ON ft.date_key = dd.date_key
  JOIN
    gold_layer.dim_products AS dp
    ON ft.product_key = dp.product_key
  WHERE
    dd.year = EXTRACT(YEAR FROM CURRENT_DATE())
    AND dd.month = EXTRACT(MONTH FROM CURRENT_DATE())
    AND (
      (ft.transaction_type IN ('CREDIT', 'TRANSFER_IN') AND dp.product_category IN ('Investment', 'Savings', 'Securities'))
      OR ft.transaction_type IN ('INVESTMENT_PURCHASE', 'BUY_STOCK')
    )
  GROUP BY
    dp.product_name,
    dp.product_category
)
SELECT
  product_name,
  product_category,
  total_investment_volume
FROM
  MonthlyInvestments
ORDER BY
  total_investment_volume DESC
LIMIT 10;

-- Query 3: Unwanted Expenses This Month
-- This query lists specific "unwanted" expenses for the current calendar month.
-- Unwanted expenses are defined by expense transaction types and specific merchant names or descriptions.
SELECT
  ft.transaction_datetime,
  ft.merchant_name,
  ft.transaction_description,
  ft.transaction_amount,
  dp.product_name AS product_associated_with_transaction
FROM
  gold_layer.fact_transactions AS ft
JOIN
  gold_layer.dim_date AS dd
  ON ft.date_key = dd.date_key
JOIN
  gold_layer.dim_products AS dp
  ON ft.product_key = dp.product_key
WHERE
  dd.year = EXTRACT(YEAR FROM CURRENT_DATE())
  AND dd.month = EXTRACT(MONTH FROM CURRENT_DATE())
  AND ft.transaction_type IN ('DEBIT', 'PAYMENT', 'TRANSFER_OUT', 'FEE') -- Expense transactions
  AND dp.product_category IS NOT NULL -- Updated based on subtask for expense definition
  AND dp.product_category NOT IN ('Investment', 'Savings', 'Securities') -- Updated based on subtask for expense definition
  AND (
    ft.merchant_name LIKE '%Gambling%'
    OR ft.transaction_description LIKE '%Online Game Purchase%'
  )
ORDER BY
  ft.transaction_datetime DESC;

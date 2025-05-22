# Import necessary libraries
import pandas as pd
import random
import string
import datetime
import os # Added for GCS functionality
from google.cloud import storage # Added for GCS functionality

# --- GCS Upload Function ---
# Ensure GCS credentials are configured in the environment.
# For example, set the GOOGLE_APPLICATION_CREDENTIALS environment variable
# to the path of your service account key file, or run in an environment
# with default credentials (e.g., GCP VM, Cloud Function, Cloud Run).

def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    """Uploads a file to the specified GCS bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        print(f"Uploading {source_file_path} to gs://{bucket_name}/{destination_blob_name}...")
        blob.upload_from_filename(source_file_path)
        print(f"File {source_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}.")
    except Exception as e:
        print(f"Error uploading {source_file_path} to GCS: {e}")

# --- Data Generation Functions ---

def generate_product_data(num_rows):
    """Generates a Pandas DataFrame for products."""
    data = []
    categories = ['Electronics', 'Clothing', 'Home Goods', 'Books', 'Sports']
    for i in range(num_rows):
        product_id = i + 1
        product_name = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(5, 20)))
        description = ' '.join([''.join(random.choices(string.ascii_lowercase, k=random.randint(3,10))) for _ in range(random.randint(5,15))])
        price = round(random.uniform(5.0, 500.0), 2)
        category = random.choice(categories)
        data.append([product_id, product_name, description, price, category])
    
    df = pd.DataFrame(data, columns=['product_id', 'product_name', 'description', 'price', 'category'])
    return df

def generate_customer_data(num_rows):
    """Generates a Pandas DataFrame for customers."""
    data = []
    for i in range(num_rows):
        customer_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        first_name = ''.join(random.choices(string.ascii_letters, k=random.randint(3, 10)))
        last_name = ''.join(random.choices(string.ascii_letters, k=random.randint(3, 10)))
        email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(['example.com', 'test.org', 'sample.net'])}"
        address = f"{random.randint(1,1000)} {''.join(random.choices(string.ascii_letters, k=random.randint(5,10)))} St, {''.join(random.choices(string.ascii_letters, k=random.randint(5,10)))} City"
        
        start_date = datetime.date(2020, 1, 1)
        end_date = datetime.date(2023, 12, 31)
        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        signup_date = start_date + datetime.timedelta(days=random_number_of_days)
        
        data.append([customer_id, first_name, last_name, email, address, signup_date])
        
    df = pd.DataFrame(data, columns=['customer_id', 'first_name', 'last_name', 'email', 'address', 'signup_date'])
    return df

def generate_transaction_data(num_rows, customer_ids, product_ids):
    """Generates a Pandas DataFrame for transactions."""
    data = []
    if not customer_ids or not product_ids:
        print("Warning: customer_ids or product_ids is empty. No transactions will be generated.")
        return pd.DataFrame(data, columns=['transaction_id', 'customer_id', 'product_id', 'quantity', 'transaction_amount', 'timestamp'])

    for i in range(num_rows):
        transaction_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        transaction_amount = round(random.uniform(10.0, 1000.0), 2) 
        
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(days=365)
        random_time = start_time + (end_time - start_time) * random.random()
        timestamp = random_time
        
        data.append([transaction_id, customer_id, product_id, quantity, transaction_amount, timestamp])
        
    df = pd.DataFrame(data, columns=['transaction_id', 'customer_id', 'product_id', 'quantity', 'transaction_amount', 'timestamp'])
    return df

# --- Main Execution Block ---
if __name__ == "__main__":
    TARGET_GCS_BUCKET = "sources-sha" # GCS bucket for uploads

    # Estimated number of rows for approximately 1MB CSVs
    num_products = 15000
    num_customers = 10000
    num_transactions = 12000

    # --- Products ---
    print(f"Generating {num_products} product rows...")
    products_df = generate_product_data(num_products)
    products_csv_path = 'products.csv'
    products_df.to_csv(products_csv_path, index=False)
    print(f"{products_csv_path} created successfully.")
    
    # Upload to GCS
    product_destination_blob = f"uploads/{os.path.basename(products_csv_path)}"
    upload_to_gcs(TARGET_GCS_BUCKET, products_csv_path, product_destination_blob)

    # --- Customers ---
    print(f"Generating {num_customers} customer rows...")
    customers_df = generate_customer_data(num_customers)
    customers_csv_path = 'customers.csv'
    customers_df.to_csv(customers_csv_path, index=False)
    print(f"{customers_csv_path} created successfully.")

    # Upload to GCS
    customer_destination_blob = f"uploads/{os.path.basename(customers_csv_path)}"
    upload_to_gcs(TARGET_GCS_BUCKET, customers_csv_path, customer_destination_blob)

    # Get actual customer_ids and product_ids for transaction generation
    customer_ids_list = customers_df['customer_id'].tolist()
    product_ids_list = products_df['product_id'].tolist()

    # --- Transactions ---
    print(f"Generating {num_transactions} transaction rows...")
    transactions_df = generate_transaction_data(num_transactions, customer_ids_list, product_ids_list)
    transactions_csv_path = 'transactions.csv'
    transactions_df.to_csv(transactions_csv_path, index=False)
    print(f"{transactions_csv_path} created successfully.")

    # Upload to GCS
    transaction_destination_blob = f"uploads/{os.path.basename(transactions_csv_path)}"
    upload_to_gcs(TARGET_GCS_BUCKET, transactions_csv_path, transaction_destination_blob)

    print("\nSample data generation and GCS upload process complete.")

    # Conceptual: Helper function to estimate CSV size (can be refined)
    # def estimate_rows_for_1mb(sample_df, target_bytes=1024*1024):
    #     if sample_df.empty or len(sample_df) < 10: 
    #         if sample_df.empty:
    #            print("Sample DataFrame is empty, cannot estimate row size.")
    #            return 0
    #         sample_csv_string = sample_df.to_csv(index=False)
    #         num_sample_rows = len(sample_df)
    #     else:
    #         sample_csv_string = sample_df.head(10).to_csv(index=False)
    #         num_sample_rows = 10
        
    #     if not sample_csv_string: 
    #         print("Could not generate CSV string from sample DataFrame.")
    #         return 0
            
    #     avg_row_size = len(sample_csv_string.encode('utf-8')) / num_sample_rows
    #     if avg_row_size == 0: 
    #         print("Average row size is zero, cannot estimate rows for 1MB.")
    #         return 0
    #     return int(target_bytes / avg_row_size)

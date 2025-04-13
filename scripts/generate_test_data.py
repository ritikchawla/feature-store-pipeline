import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def generate_transaction_data(n_customers=100, n_transactions=1000):
    """Generate synthetic transaction data"""
    
    # Generate customer IDs
    customer_ids = np.random.randint(1, n_customers + 1, size=n_transactions)
    
    # Generate timestamps over last 60 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=60)
    timestamps = pd.date_range(start=start_date, end=end_date, periods=n_transactions)
    
    # Generate transaction amounts (between $1 and $1000)
    amounts = np.random.uniform(1, 1000, size=n_transactions)
    
    # Create DataFrame
    df = pd.DataFrame({
        'transaction_id': range(1, n_transactions + 1),
        'customer_id': customer_ids,
        'transaction_timestamp': timestamps,
        'transaction_amount': amounts
    })
    
    return df

def save_test_data():
    """Generate and save test transaction data"""
    
    # Create data directory if it doesn't exist
    os.makedirs('data/raw/transactions', exist_ok=True)
    
    # Generate data
    df = generate_transaction_data()
    
    # Save as parquet
    output_path = 'data/raw/transactions/test_transactions.parquet'
    df.to_parquet(output_path, index=False)
    
    print(f"Generated test data saved to {output_path}")
    print(f"Number of transactions: {len(df)}")
    print(f"Number of unique customers: {df['customer_id'].nunique()}")
    print("\nSample data:")
    print(df.head())

if __name__ == "__main__":
    save_test_data()
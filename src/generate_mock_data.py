"""
Module: generate_mock_data.py
Description: Generates a simulated telecom log dataset for local PySpark testing.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def generate_telecom_data(num_rows=50000, output_path="C:\\Users\\SURYADIP\\Desktop\\SparkScale Churn\\SparkScale-Churn\\data\\raw_telecom_logs.csv"):
    print(f"Generating {num_rows} rows of mock telecom data...")
    
    # 1. Generate Base Data
    np.random.seed(42) # For reproducibility
    user_ids = [f"USER_{np.random.randint(1000, 5000)}" for _ in range(num_rows)]
    
    # Generate random timestamps over the last 30 days
    base_time = datetime.now()
    timestamps = [base_time - timedelta(minutes=np.random.randint(0, 43200)) for _ in range(num_rows)]
    
    call_duration = np.random.uniform(0.5, 120.0, num_rows)
    data_usage = np.random.uniform(10.0, 5000.0, num_rows)
    complaints = np.random.choice([0, 1], num_rows, p=[0.95, 0.05]) # 5% complaint rate
    churn_labels = np.random.choice([0, 1], num_rows, p=[0.90, 0.10]) # 10% churn rate

    # 2. Inject "Messy" Data (Simulate production log failures)
    # Randomly make 5% of call durations and data usage null (NaN)
    call_duration[np.random.choice(num_rows, int(num_rows * 0.05), replace=False)] = np.nan
    data_usage[np.random.choice(num_rows, int(num_rows * 0.05), replace=False)] = np.nan

    # 3. Create DataFrame
    df = pd.DataFrame({
        "user_id": user_ids,
        "timestamp": timestamps,
        "call_duration_mins": np.round(call_duration, 2),
        "data_usage_mb": np.round(data_usage, 2),
        "complaint_flag": complaints,
        "churn_label": churn_labels
    })

    # Ensure the data directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # 4. Save to CSV
    df.to_csv(output_path, index=False)
    print(f"✅ Successfully saved mock data to: {output_path}")

if __name__ == "__main__":
    generate_telecom_data()
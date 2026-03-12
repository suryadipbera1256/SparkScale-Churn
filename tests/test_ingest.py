"""
Module: test_ingest.py
Description: Unit tests for the PySpark ingestion module.
"""
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from src.ingest import SparkDataIngestor

def test_clean_data_handles_nulls(spark):
    """
    Tests if the ETL pipeline correctly fills missing numeric values 
    and drops rows with null user_ids.
    """
    # --- PYTHON 3.14 BYPASS ---
    # Force Spark to use PyArrow (C++ memory) instead of CloudPickle 
    # to serialize the test data, bypassing the RecursionError stack overflow.
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    # --------------------------

    # 1. ARRANGE
    ingestor = SparkDataIngestor("Test_ETL")
    ingestor.spark = spark 
    
    # Create data using Pandas instead of PySpark Rows to avoid pickling
    mock_data_pd = pd.DataFrame([
        {"user_id": "USER_1", "timestamp": None, "call_duration_mins": 10.0, "data_usage_mb": None, "complaint_flag": None, "churn_label": 0},
        {"user_id": None, "timestamp": None, "call_duration_mins": 5.0, "data_usage_mb": 100.0, "complaint_flag": 0, "churn_label": 1}
    ])
    
    loose_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("call_duration_mins", FloatType(), True),
        StructField("data_usage_mb", FloatType(), True),
        StructField("complaint_flag", IntegerType(), True),
        StructField("churn_label", IntegerType(), True)
    ])
    
    # Ingest the Pandas DataFrame directly via Arrow
    raw_df = spark.createDataFrame(mock_data_pd, schema=loose_schema)
    
    # 2. ACT: Pass the dirty data through your cleansing logic
    clean_df = ingestor.clean_data(raw_df)
    results = clean_df.collect()
    
    # 3. ASSERT: Prove the logic executed correctly
    assert len(results) == 1
    assert results[0].data_usage_mb == 0.0
    assert results[0].complaint_flag == 0
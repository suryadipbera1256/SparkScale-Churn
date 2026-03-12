"""
Module: config.py
Description: Centralized configuration and environment variables.
"""
import os

# Define absolute paths based on the container environment
BASE_DIR = "/opt/spark"
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_PATH = os.path.join(DATA_DIR, "raw_telecom_logs.csv")

# Spark Cluster Configurations
SPARK_MASTER_URL = "spark://spark-master:7077"
EXECUTOR_MEMORY = "4g"

# Machine Learning Parameters
RANDOM_SEED = 42
RF_NUM_TREES = 50
RF_MAX_DEPTH = 5
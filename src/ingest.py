# """
# Module: ingest.py
# Description: Distributed ETL pipeline for ingesting and cleansing telecom log data.
# Author: Suryadip Bera
# """

# import logging
# from pyspark.sql import SparkSession, DataFrame
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
# from pyspark.sql.functions import col, isnan, when, count

# # Configure basic logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# class SparkDataIngestor:
#     def __init__(self, app_name: str = "SparkScale_Churn_ETL"):
#         """
#         Initializes the SparkSession. Connecting to the Docker Spark Master.
#         """
#         logger.info(f"Initializing Spark Session: {app_name}")
#         self.spark = SparkSession.builder \
#             .appName(app_name) \
#             .master("spark://spark-master:7077") \
#             .config("spark.executor.memory", "4g") \
#             .getOrCreate()

#     def define_schema(self) -> StructType:
#         """
#         Defines an explicit schema. 
#         Industry Practice: Never use inferSchema=True for TB-scale data as it requires a full data pass.
#         """
#         schema = StructType([
#             StructField("user_id", StringType(), False),
#             StructField("timestamp", TimestampType(), True),
#             StructField("call_duration_mins", FloatType(), True),
#             StructField("data_usage_mb", FloatType(), True),
#             StructField("complaint_flag", IntegerType(), True),
#             StructField("churn_label", IntegerType(), True) # 1 for Churn, 0 for Retained
#         ])
#         return schema

#     def load_data(self, file_path: str) -> DataFrame:
#         """
#         Loads raw data into a Spark DataFrame using the explicit schema.
#         """
#         logger.info(f"Attempting to load data from: {file_path}")
#         try:
#             schema = self.define_schema()
#             df = self.spark.read.csv(file_path, header=True, schema=schema)
            
#             # Validation: Log the row count to ensure successful ingestion
#             row_count = df.count()
#             logger.info(f"Successfully loaded {row_count} rows.")
#             return df
            
#         except Exception as e:
#             logger.error(f"Failed to load data: {str(e)}")
#             raise

#     def clean_data(self, df: DataFrame) -> DataFrame:
#         """
#         Performs initial data cleansing: Handling nulls and missing values.
#         """
#         logger.info("Starting data cleansing process...")
        
#         # Drop rows where critical identifier 'user_id' is null
#         cleaned_df = df.dropna(subset=["user_id"])
        
#         # Fill missing numeric values with 0.0 (e.g., no data usage means 0 usage)
#         cleaned_df = cleaned_df.fillna({
#             "call_duration_mins": 0.0,
#             "data_usage_mb": 0.0,
#             "complaint_flag": 0
#         })
        
#         logger.info("Data cleansing complete.")
#         return cleaned_df

#     def close_session(self):
#         """Terminates the Spark session to free up cluster resources."""
#         self.spark.stop()
#         logger.info("Spark Session closed.")

# if __name__ == "__main__":
#     # Execution Block
#     ingestor = SparkDataIngestor()
    
#     # Path maps to the volume mounted in our docker-compose.yml
#     DATA_PATH = "/opt/spark/data/raw_telecom_logs.csv" 
    
#     try:
#         # Step 1: Ingest 
#         raw_df = ingestor.load_data(DATA_PATH)
        
#         # Step 2: Clean 
#         clean_df = ingestor.clean_data(raw_df)
        
#         # Step 3: Schema Validation 
#         clean_df.printSchema()
#         clean_df.show(5)
        
#     except Exception as e:
#         logger.error("Pipeline failed.")
#     finally:
#         ingestor.close_session()


"""
Module: ingest.py
Description: Distributed ETL pipeline for ingesting and cleansing telecom log data.
Author: Suryadip Bera
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# --- IMPORT ENTERPRISE STANDARDS ---
from config import SPARK_MASTER_URL, EXECUTOR_MEMORY, RAW_DATA_PATH
from logger import get_custom_logger

# Initialize the standardized logger for this specific module
logger = get_custom_logger("SparkDataIngestor")

class SparkDataIngestor:
    def __init__(self, app_name: str = "SparkScale_Churn_ETL"):
        """
        Initializes the SparkSession using centralized configurations.
        """
        logger.info(f"Initializing Spark Session: {app_name}")
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master(SPARK_MASTER_URL) \
            .config("spark.executor.memory", EXECUTOR_MEMORY) \
            .getOrCreate()

    def define_schema(self) -> StructType:
        """
        Defines an explicit schema. 
        Industry Practice: Never use inferSchema=True for TB-scale data.
        """
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("timestamp", TimestampType(), True),
            StructField("call_duration_mins", FloatType(), True),
            StructField("data_usage_mb", FloatType(), True),
            StructField("complaint_flag", IntegerType(), True),
            StructField("churn_label", IntegerType(), True) # 1 for Churn, 0 for Retained
        ])
        return schema

    def load_data(self, file_path: str) -> DataFrame:
        """
        Loads raw data into a Spark DataFrame using the explicit schema.
        """
        logger.info(f"Attempting to load data from: {file_path}")
        try:
            schema = self.define_schema()
            df = self.spark.read.csv(file_path, header=True, schema=schema)
            
            # Validation: Log the row count to ensure successful ingestion
            row_count = df.count()
            logger.info(f"Successfully loaded {row_count} rows.")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}")
            raise

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Performs initial data cleansing: Handling nulls and missing values.
        """
        logger.info("Starting data cleansing process...")
        
        # Drop rows where critical identifier 'user_id' is null
        cleaned_df = df.dropna(subset=["user_id"])
        
        # Fill missing numeric values with 0.0
        cleaned_df = cleaned_df.fillna({
            "call_duration_mins": 0.0,
            "data_usage_mb": 0.0,
            "complaint_flag": 0
        })
        
        logger.info("Data cleansing complete.")
        return cleaned_df

    def close_session(self):
        """Terminates the Spark session to free up cluster resources."""
        self.spark.stop()
        logger.info("Spark Session closed.")

if __name__ == "__main__":
    # Execution Block
    ingestor = SparkDataIngestor()
    
    try:
        # Step 1: Ingest using the dynamic path from config.py
        raw_df = ingestor.load_data(RAW_DATA_PATH)
        
        # Step 2: Clean
        clean_df = ingestor.clean_data(raw_df)
        
        # Step 3: Schema Validation
        clean_df.printSchema()
        clean_df.show(5)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
    finally:
        ingestor.close_session()
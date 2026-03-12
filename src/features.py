"""
Module: features.py
Description: Distributed Feature Engineering using PySpark and VectorAssembler.
Author: Suryadip Bera
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler

# --- IMPORT ENTERPRISE STANDARDS ---
from config import SPARK_MASTER_URL, RAW_DATA_PATH
from logger import get_custom_logger

# Initialize the standardized logger for this specific module
logger = get_custom_logger("SparkFeatureEngineer")

class SparkFeatureEngineer:
    def __init__(self, app_name: str = "SparkScale_Churn_Features"):
        logger.info(f"Initializing Spark Session: {app_name}")
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master(SPARK_MASTER_URL) \
            .getOrCreate()

    def engineer_user_aggregates(self, df: DataFrame) -> DataFrame:
        """Groups raw log events by user_id and calculates aggregate features."""
        logger.info("Calculating user-level aggregates...")
        
        user_features_df = df.groupBy("user_id").agg(
            F.sum("call_duration_mins").alias("total_call_duration"),
            F.avg("data_usage_mb").alias("avg_data_usage"),
            F.sum("complaint_flag").alias("total_complaints"),
            F.max("churn_label").alias("label") 
        )
        
        return user_features_df.fillna(0.0)

    def assemble_vectors(self, df: DataFrame) -> DataFrame:
        """Combines numerical features into a single 'features' vector."""
        logger.info("Assembling feature vectors...")
        
        feature_columns = ["total_call_duration", "avg_data_usage", "total_complaints"]
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        
        return assembler.transform(df)

    def close_session(self):
        self.spark.stop()
        logger.info("Spark Session closed.")

if __name__ == "__main__":
    from ingest import SparkDataIngestor
    
    ingestor = SparkDataIngestor("SparkScale_Feature_Pipeline")
    
    try:
        raw_df = ingestor.load_data(RAW_DATA_PATH)
        clean_df = ingestor.clean_data(raw_df)
        
        engineer = SparkFeatureEngineer()
        aggregated_df = engineer.engineer_user_aggregates(clean_df)
        ml_ready_df = engineer.assemble_vectors(aggregated_df)
        
        logger.info("Final Machine Learning Ready Schema:")
        ml_ready_df.printSchema()
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
    finally:
        ingestor.close_session()
        if 'engineer' in locals():
            engineer.close_session()
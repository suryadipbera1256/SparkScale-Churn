"""
Module: train_model.py
Description: Distributed Machine Learning using PySpark MLlib.
Author: Suryadip Bera
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# --- IMPORT ENTERPRISE STANDARDS ---
from config import RAW_DATA_PATH, RANDOM_SEED, RF_NUM_TREES, RF_MAX_DEPTH
from logger import get_custom_logger

logger = get_custom_logger("SparkChurnPredictor")

class SparkChurnPredictor:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def train_evaluate_model(self, ml_ready_df: DataFrame):
        logger.info("Splitting data into 80% Training and 20% Testing...")
        train_data, test_data = ml_ready_df.randomSplit([0.8, 0.2], seed=RANDOM_SEED)
        
        logger.info("Initializing Distributed Random Forest Classifier...")
        rf = RandomForestClassifier(
            featuresCol="features", 
            labelCol="label", 
            numTrees=RF_NUM_TREES,  
            maxDepth=RF_MAX_DEPTH,   
            seed=RANDOM_SEED
        )
        
        logger.info("Training the model across the cluster...")
        rf_model = rf.fit(train_data)
        
        logger.info("Generating predictions on the test dataset...")
        predictions = rf_model.transform(test_data)
        
        logger.info("Evaluating model performance...")
        auc_evaluator = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
        )
        auc = auc_evaluator.evaluate(predictions)
        
        acc_evaluator = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="accuracy"
        )
        accuracy = acc_evaluator.evaluate(predictions)
        
        logger.info("=========================================")
        logger.info("       FINAL MODEL METRICS               ")
        logger.info("=========================================")
        logger.info(f"Area Under ROC (AUC) : {auc:.4f}")
        logger.info(f"Overall Accuracy     : {accuracy:.4f}")
        logger.info("=========================================")
        
        return rf_model

if __name__ == "__main__":
    from ingest import SparkDataIngestor
    from features import SparkFeatureEngineer
    
    ingestor = SparkDataIngestor("SparkScale_EndToEnd_Pipeline")
    
    try:
        raw_df = ingestor.load_data(RAW_DATA_PATH)
        clean_df = ingestor.clean_data(raw_df)
        
        engineer = SparkFeatureEngineer()
        aggregated_df = engineer.engineer_user_aggregates(clean_df)
        ml_ready_df = engineer.assemble_vectors(aggregated_df)
        
        predictor = SparkChurnPredictor(ingestor.spark)
        rf_model = predictor.train_evaluate_model(ml_ready_df)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
    finally:
        ingestor.close_session()
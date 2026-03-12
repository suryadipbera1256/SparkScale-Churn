@echo off
echo ========================================================
echo Starting SparkScale Churn Automated Pipeline...
echo ========================================================

:: Step 1: Spin up the Docker Cluster in the background
echo [1/4] Starting Docker Spark Cluster...
docker-compose up -d

:: Step 2: Give the cluster 15 seconds to fully boot up and connect
echo [2/4] Waiting for Master and Worker nodes to initialize...
timeout /t 15 /nobreak > NUL

:: Step 3: Trigger the Master Node to run the End-to-End Pipeline
:: (Because train_model.py imports Week 1 and Week 2, this runs everything)
echo [3/4] Submitting PySpark Job to the Cluster...
docker exec sparkscale-churn-spark-master-1 spark-submit /opt/spark/src/train_model.py

:: Step 4: Shut down the cluster to free up system resources
echo [4/4] Pipeline complete. Shutting down cluster...
docker-compose down

echo ========================================================
echo Pipeline Execution Finished Successfully.
echo ========================================================
pause
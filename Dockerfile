# 1. Start from our stable Spark base image
FROM bitnamilegacy/spark:3.4.1

# 2. Switch to root user temporarily to install system-level packages
USER root

# 3. Install the Data Science dependencies required by Spark MLlib
# (Matching the versions from our local requirements.txt)
RUN pip install --no-cache-dir numpy==1.26.2 pandas==2.1.4 pyarrow==14.0.1

# 4. Switch back to the standard non-root user (1001) for security, 
# which is the strict requirement for Bitnami images.
USER 1001
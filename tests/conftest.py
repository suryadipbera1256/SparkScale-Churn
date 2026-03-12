"""
Module: conftest.py
Description: Pytest configuration and fixtures for PySpark testing.
"""
import sys
import os
import typing
import types

# --- PIPELINE PATH INJECTION ---
# Tells local pytest to look inside the 'src' folder for modules
# so we don't have to break our Docker-ready import statements.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
# -------------------------------

# --- BULLETPROOF HOTFIX FOR PYTHON 3.14 ---
# Python 3.14 strictly forbids dot notation on single modules.
# We explicitly construct a fake 'typing.io' module in memory.
if 'typing.io' not in sys.modules:
    typing_io = types.ModuleType('typing.io')
    typing_io.BinaryIO = typing.BinaryIO
    sys.modules['typing.io'] = typing_io
# ------------------------------------------

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Creates a local, temporary Spark session for unit testing."""
    spark = SparkSession.builder \
        .appName("SparkScale_Test_Env") \
        .master("local[2]") \
        .getOrCreate()
    
    yield spark
    
    # Tear down the session after tests finish
    spark.stop()
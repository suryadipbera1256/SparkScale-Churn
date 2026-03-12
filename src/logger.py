"""
Module: logger.py
Description: Custom enterprise logging configuration.
"""
import logging
import os
import sys

def get_custom_logger(name: str) -> logging.Logger:
    """Creates a standardized logger that outputs to both console and file."""
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent duplicate logs if logger is called multiple times
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create a standardized format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 1. Console Handler (Prints to your terminal)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
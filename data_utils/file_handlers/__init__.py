import os
import logging

def list_files_in_directory(directory:str) -> list:
    files = os.listdir(directory)
    logging.info(f"Found {len()}")


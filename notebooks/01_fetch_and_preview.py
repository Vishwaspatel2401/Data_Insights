#!/usr/bin/env python3
"""
Data fetching and preview script
Converted from 01_fetch_and_preview.ipynb
"""

import sys
import os
import importlib
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src import fetch_data
importlib.reload(fetch_data)

from src.fetch_data import list_tables
import delta_sharing
import time
from datetime import datetime
import pandas as pd
from src.config import RAW_DIR


def robust_fetch_table(share, schema, table, max_retries=3, delay=2):
    """
    Robust table fetching with retry logic for authentication issues
    """
    for attempt in range(max_retries):
        try:
            print(f"  Attempt {attempt + 1}/{max_retries}...")
            
            credential_path = 'credentials/config.share'
            table_url = f"{credential_path}#{share}.{schema}.{table}"
            
            df = delta_sharing.load_as_pandas(table_url)
            return df

        except Exception as e:
            error_msg = str(e)
            if "FileNotFoundError" in str(type(e)) or "401" in error_msg or "NoAuthenticationInformation" in error_msg:
                if attempt < max_retries - 1:
                    print(f"    Authentication/URL issue {e}, retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                else:
                    print(f"    Final attempt failed: {type(e).__name__}")
                    raise e
            else:
                print(f"    Non-retryable error: {type(e).__name__}: {error_msg[:100]}...")
                raise e
    
    return None


def main():
    """Main execution function"""
    # Step 1: List available tables
    print(" Listing available tables...")
    tables = list_tables()
    print(f"Found {len(tables)} tables:")
    for t in tables:
        print(f"  - {t.share}.{t.schema}.{t.name}")


if __name__ == "__main__":
    main()

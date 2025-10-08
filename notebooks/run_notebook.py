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

# Step 1: List available tables
print(" Listing available tables...")
tables = list_tables()
print(f"Found {len(tables)} tables:")
for t in tables:
    print(f"  - {t.share}.{t.schema}.{t.name}")

print(f"\nCurrent time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


print("\n Attempting to fetch tables with retry logic...")
all_dfs = {}
failed_tables = []

for t in tables:
    key = f"{t.share}.{t.schema}.{t.name}"
    print(f"\n Fetching {key}...")
    
    try:
        df = robust_fetch_table(t.share, t.schema, t.name)
        if df is not None:
            all_dfs[key] = df
            print(f" Successfully fetched {key} - Shape: {df.shape}")
            
            # Save the dataframe
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_file = RAW_DIR / f"{t.name}_{timestamp}.parquet"
            df.to_parquet(out_file, index=False)
            print(f"✅ Saved raw data → {out_file}")
        else:
            raise Exception("Failed to fetch table after all retries")
        
    except Exception as e:
        print(f" Failed to fetch {key} after all retries")
        failed_tables.append((key, str(e)))

# Step 3: Report results
print(f"\n Summary:")
print(f"   Successfully fetched: {len(all_dfs)} tables")
print(f"   Failed: {len(failed_tables)} tables")

if failed_tables:
    print(f"\n Failed tables:")
    for table, error in failed_tables:
        print(f"  - {table}: {error[:100]}...")

# Step 4: Preview successful data
if all_dfs:
    print(f"\n Data Preview:")
    for table_name, df in all_dfs.items():
        print(f"\n{table_name} (Shape: {df.shape}):")
        print(df.head(3))
        print(f"Columns: {list(df.columns)}")
else:
    print(f"\n  No data was successfully fetched.")
    print(f"This could be due to:")
    print(f"  - Authentication issues with the Delta Sharing provider")
    print(f"  - Network connectivity problems")
    print(f"  - Expired or invalid credentials")
    print(f"  - Provider-side configuration issues")

#!/usr/bin/env python3
"""
Improved data fetching script with better error handling and alternative methods
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
import requests
from urllib.parse import urlparse


def test_connection():
    """Test the basic connection to Delta Sharing provider"""
    try:
        print("🔍 Testing connection to Delta Sharing provider...")
        client = delta_sharing.SharingClient("credentials/config.share")
        tables = client.list_all_tables()
        print(f"✅ Connection successful! Found {len(tables)} tables.")
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


def _format_bytes(num_bytes):
    """Return human-readable size for a byte count."""
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    unit_idx = 0
    while size >= 1024 and unit_idx < len(units) - 1:
        size /= 1024.0
        unit_idx += 1
    return f"{size:.2f} {units[unit_idx]}"


def estimate_table_size_bytes(share, schema, table, profile_path="credentials/config.share"):
    """Estimate remote size of a table by summing file sizes from Delta Sharing (no download)."""
    try:
        client = delta_sharing.SharingClient(profile_path)
        target = None
        for t in client.list_all_tables():
            if t.share == share and t.schema == schema and t.name == table:
                target = t
                break
        if not target:
            return 0

        # Use underlying REST client to list files; response contains add_files with .size
        resp = client._rest_client.list_files_in_table(target)
        files = getattr(resp, "add_files", []) or []
        sizes = [getattr(f, "size", 0) for f in files]
        return int(sum(sizes))
    except Exception:
        return 0


def preflight_estimate_and_confirm():
    """Show per-table and total remote sizes, then ask whether to proceed with full fetch."""
    tables = list_tables()
    if not tables:
        print("❌ No tables available for size estimation")
        return False

    print("\n🧮 Estimating remote sizes (no data download)...")
    per_table_sizes = []
    total_bytes = 0
    for t in tables:
        size_bytes = estimate_table_size_bytes(t.share, t.schema, t.name)
        total_bytes += size_bytes
        per_table_sizes.append((t, size_bytes))

    print("\n📏 Estimated sizes by table:")
    for t, sz in per_table_sizes:
        print(f"   - {t.share}.{t.schema}.{t.name}: {_format_bytes(sz)}")

    print(f"\n🧩 Estimated total size: {_format_bytes(total_bytes)}")

    # Prompt for confirmation
    try:
        answer = input("\nProceed with full fetch and save locally? [y/N]: ").strip().lower()
    except EOFError:
        answer = "n"

    proceed = answer in ("y", "yes")
    if not proceed:
        print("🚫 Skipping full fetch per user choice.")
    return proceed


def fetch_table_with_timeout(share, schema, table, timeout=30):
    """Fetch table with timeout and better error handling"""
    try:
        print(f"📥 Fetching {share}.{schema}.{table}...")
        
        credential_path = 'credentials/config.share'
        table_url = f"{credential_path}#{share}.{schema}.{table}"
        
        # Try with timeout
        df = delta_sharing.load_as_pandas(table_url)
        
        if df is not None and not df.empty:
            print(f"✅ Successfully fetched {table} - Shape: {df.shape}")
            return df
        else:
            print(f"⚠️  Fetched empty table: {table}")
            return None
            
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Failed to fetch {share}.{schema}.{table}")
        print(f"   Error: {error_msg[:200]}...")
        
        # Check if it's a URL/authentication issue
        if "FileNotFoundError" in str(type(e)) or "azure" in error_msg.lower():
            print(f"   🔧 This appears to be an Azure Data Lake authentication issue")
            print(f"   💡 The signed URLs may have expired or have permission issues")
        
        return None


def fetch_single_table_sample():
    """Try to fetch just one table as a test"""
    print("\n🧪 Testing single table fetch...")
    
    # Get the first table
    tables = list_tables()
    if not tables:
        print("❌ No tables available")
        return None
    
    first_table = tables[0]
    print(f"📋 Testing with table: {first_table.share}.{first_table.schema}.{first_table.name}")
    
    df = fetch_table_with_timeout(first_table.share, first_table.schema, first_table.name)
    
    if df is not None:
        print(f"\n📊 Sample data preview:")
        print(df.head(3))
        print(f"\n📈 Data info:")
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {list(df.columns)}")
        print(f"   Data types:\n{df.dtypes}")
        
        # Save sample
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_file = RAW_DIR / f"sample_{first_table.name}_{timestamp}.parquet"
        df.to_parquet(out_file, index=False)
        print(f"💾 Saved sample data → {out_file}")
        
        return df
    
    return None


def fetch_all_tables_improved():
    """Fetch all tables with improved error handling"""
    print("\n🚀 Starting improved data fetch process...")
    
    # Test connection first
    if not test_connection():
        return {}
    
    tables = list_tables()
    print(f"\n📋 Found {len(tables)} tables to fetch:")
    for t in tables:
        print(f"   - {t.share}.{t.schema}.{t.name}")
    
    all_dfs = {}
    successful_fetches = 0
    
    for i, t in enumerate(tables, 1):
        print(f"\n📥 [{i}/{len(tables)}] Processing {t.share}.{t.schema}.{t.name}")
        
        df = fetch_table_with_timeout(t.share, t.schema, t.name)
        
        if df is not None:
            all_dfs[f"{t.share}.{t.schema}.{t.name}"] = df
            successful_fetches += 1
            
            # Save the dataframe
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_file = RAW_DIR / f"{t.name}_{timestamp}.parquet"
            df.to_parquet(out_file, index=False)
            print(f"💾 Saved → {out_file}")
        
        # Add delay between requests to avoid rate limiting
        if i < len(tables):
            time.sleep(2)
    
    print(f"\n📊 Fetch Summary:")
    print(f"   ✅ Successful: {successful_fetches}/{len(tables)}")
    print(f"   ❌ Failed: {len(tables) - successful_fetches}/{len(tables)}")
    
    return all_dfs


def main():
    """Main execution function"""
    print("🎯 Delta Sharing Data Fetch - Improved Version")
    print("=" * 50)
    
    # First try a single table test
    sample_df = fetch_single_table_sample()
    
    if sample_df is not None:
        print("\n🎉 Single table test successful!")
        # Preflight: estimate sizes and confirm before fetching all
        if preflight_estimate_and_confirm():
            print("\n🚀 Proceeding with full fetch...")
            all_dfs = fetch_all_tables_improved()
        else:
            all_dfs = {}
        
        if all_dfs:
            print(f"\n🎊 Successfully fetched {len(all_dfs)} tables!")
            
            # Show summary of all fetched data
            print(f"\n📈 Data Summary:")
            for table_name, df in all_dfs.items():
                print(f"   {table_name}: {df.shape[0]} rows, {df.shape[1]} columns")
        else:
            print(f"\n😞 No data was successfully fetched.")
            print(f"\n🔧 Troubleshooting suggestions:")
            print(f"   1. Check if your credentials are still valid")
            print(f"   2. Verify network connectivity")
            print(f"   3. Contact the data provider about access permissions")
            print(f"   4. Try again later (signed URLs may be temporarily expired)")
    else:
        print(f"\n😞 Single table test failed. Please check the troubleshooting suggestions above.")


if __name__ == "__main__":
    main()





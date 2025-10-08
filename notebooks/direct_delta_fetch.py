#!/usr/bin/env python3
"""
Direct Delta Sharing fetch with alternative authentication methods
"""

import sys
import os
import importlib
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import delta_sharing
import pandas as pd
from datetime import datetime
from src.config import RAW_DIR
import json


def load_credentials():
    """Load and validate credentials"""
    try:
        with open('credentials/config.share', 'r') as f:
            creds = json.load(f)
        
        print(f"📋 Credentials loaded:")
        print(f"   Endpoint: {creds['endpoint']}")
        print(f"   Expiration: {creds['expirationTime']}")
        print(f"   Bearer Token: {creds['bearerToken'][:20]}...")
        
        return creds
    except Exception as e:
        print(f"❌ Failed to load credentials: {e}")
        return None


def test_direct_client():
    """Test direct client connection"""
    try:
        print("\n🔍 Testing direct Delta Sharing client...")
        client = delta_sharing.SharingClient("credentials/config.share")
        
        # Try to get table metadata first
        tables = client.list_all_tables()
        print(f"✅ Client connection successful! Found {len(tables)} tables.")
        
        for table in tables:
            print(f"   📋 {table.share}.{table.schema}.{table.name}")
            print(f"      - History: {hasattr(table, 'history')}")
            print(f"      - Protocol: {hasattr(table, 'protocol')}")
        
        return client, tables
    except Exception as e:
        print(f"❌ Direct client test failed: {e}")
        return None, []


def try_alternative_fetch_methods(client, tables):
    """Try different methods to fetch data"""
    print(f"\n🔄 Trying alternative fetch methods...")
    
    if not tables:
        print("❌ No tables available")
        return {}
    
    successful_dfs = {}
    
    for i, table in enumerate(tables[:2]):  # Try first 2 tables only
        table_name = f"{table.share}.{table.schema}.{table.name}"
        print(f"\n📥 [{i+1}/2] Trying to fetch: {table_name}")
        
        # Method 1: Direct load_as_pandas
        try:
            print("   🔧 Method 1: Direct load_as_pandas")
            table_url = f"credentials/config.share#{table.share}.{table.schema}.{table.name}"
            df = delta_sharing.load_as_pandas(table_url)
            
            if df is not None and not df.empty:
                print(f"   ✅ Success! Shape: {df.shape}")
                successful_dfs[table_name] = df
                continue
            else:
                print(f"   ⚠️  Empty result")
                
        except Exception as e:
            print(f"   ❌ Method 1 failed: {str(e)[:100]}...")
        
        # Method 2: Try with absolute path
        try:
            print("   🔧 Method 2: Absolute path")
            abs_path = str(Path("credentials/config.share").absolute())
            table_url = f"{abs_path}#{table.share}.{table.schema}.{table.name}"
            df = delta_sharing.load_as_pandas(table_url)
            
            if df is not None and not df.empty:
                print(f"   ✅ Success! Shape: {df.shape}")
                successful_dfs[table_name] = df
                continue
            else:
                print(f"   ⚠️  Empty result")
                
        except Exception as e:
            print(f"   ❌ Method 2 failed: {str(e)[:100]}...")
        
        # Method 3: Try using client methods
        try:
            print("   🔧 Method 3: Client methods")
            # This might not work but let's try
            if hasattr(client, 'get_table_metadata'):
                metadata = client.get_table_metadata(table)
                print(f"   📋 Metadata: {metadata}")
            
        except Exception as e:
            print(f"   ❌ Method 3 failed: {str(e)[:100]}...")
        
        print(f"   😞 All methods failed for {table_name}")
    
    return successful_dfs


def save_results(successful_dfs):
    """Save successful fetches"""
    if not successful_dfs:
        print(f"\n😞 No data was successfully fetched")
        return
    
    print(f"\n💾 Saving {len(successful_dfs)} successful fetches...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for table_name, df in successful_dfs.items():
        # Extract table name for filename
        table_short = table_name.split('.')[-1]
        out_file = RAW_DIR / f"{table_short}_{timestamp}.parquet"
        
        try:
            df.to_parquet(out_file, index=False)
            print(f"   ✅ Saved {table_name} → {out_file}")
            print(f"      Shape: {df.shape}, Columns: {list(df.columns)}")
        except Exception as e:
            print(f"   ❌ Failed to save {table_name}: {e}")


def main():
    """Main execution function"""
    print("🎯 Direct Delta Sharing Fetch")
    print("=" * 40)
    
    # Load credentials
    creds = load_credentials()
    if not creds:
        return
    
    # Test client
    client, tables = test_direct_client()
    if not client:
        return
    
    # Try alternative methods
    successful_dfs = try_alternative_fetch_methods(client, tables)
    
    # Save results
    save_results(successful_dfs)
    
    # Summary
    if successful_dfs:
        print(f"\n🎉 Successfully fetched {len(successful_dfs)} tables!")
        print(f"\n📊 Data Preview:")
        for table_name, df in successful_dfs.items():
            print(f"\n{table_name}:")
            print(df.head(3))
    else:
        print(f"\n😞 No data could be fetched.")
        print(f"\n🔧 This appears to be an Azure Data Lake authentication issue.")
        print(f"   The signed URLs in the Delta Sharing response are failing.")
        print(f"   Possible solutions:")
        print(f"   1. Contact the data provider to refresh the sharing permissions")
        print(f"   2. Check if your bearer token needs to be refreshed")
        print(f"   3. Verify network connectivity to Azure Data Lake")
        print(f"   4. Try again later (URLs may be temporarily expired)")


if __name__ == "__main__":
    main()



#!/usr/bin/env python3
"""
Memory-safe Delta Sharing fetcher for ALL tables from a Delta Sharing endpoint.
Streams each Parquet file per table, converts to CSV, saves locally,
and optionally builds per-table Dask DataFrames.
"""

import sys
import gc
import time
import io
import requests
from pathlib import Path
from datetime import datetime

# Add project root to sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import delta_sharing
import pandas as pd
import pyarrow.parquet as pq
import dask.dataframe as dd
from src.config import RAW_DIR


# --------------------------------------------------------------------------- #
# Utility Helpers
# --------------------------------------------------------------------------- #
def format_bytes(num_bytes):
    """Return human-readable size for a byte count."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.2f} TB"


def get_memory_usage():
    """Return current memory usage in MB."""
    import psutil
    process = psutil.Process()
    return process.memory_info().rss / (1024 * 1024)


# --------------------------------------------------------------------------- #
# Core Logic for Fetching One Table
# --------------------------------------------------------------------------- #
def fetch_table_files(client, table, base_dir: Path):
    """
    Fetches all files for a single Delta Sharing table.
    Saves each Parquet file as CSV and builds a Dask DataFrame.
    """
    table_name = f"{table.share}.{table.schema}.{table.name}"
    print(f"\n📊 TABLE: {table_name}")
    print("=" * 80)

    try:
        # List physical files (Parquet partitions)
        files_resp = client._rest_client.list_files_in_table(table)
        files = getattr(files_resp, "add_files", []) or []
        print(f"📁 Found {len(files)} files")

        if not files:
            print("⚠️  No files found for this table.")
            return None

        table_dir = base_dir / f"{table.name}"
        table_dir.mkdir(parents=True, exist_ok=True)

        saved_files = []

        for i, f in enumerate(files, start=1):
            size = getattr(f, "size", 0)
            url = getattr(f, "url", None)
            print(f"\n📄 Processing file {i}/{len(files)} ({format_bytes(size)})")

            try:
                # Stream download to memory buffer
                with requests.get(url, stream=True, timeout=300) as r:
                    r.raise_for_status()
                    buf = io.BytesIO(r.content)

                # Convert to Arrow / Pandas
                table_arrow = pq.read_table(buf)
                df_chunk = table_arrow.to_pandas(types_mapper=pd.ArrowDtype)

                print(f"   ✅ Loaded chunk shape: {df_chunk.shape}")
                print(f"   💾 Memory usage: {get_memory_usage():.1f} MB")

                # Save locally as CSV
                file_path = table_dir / f"{table.name}_part_{i:02d}.csv"
                df_chunk.to_csv(file_path, index=False)
                saved_files.append(file_path)
                print(f"   📦 Saved → {file_path.name}")

                # Cleanup
                del df_chunk, table_arrow, buf
                gc.collect()
                time.sleep(0.2)

            except Exception as e:
                print(f"   ❌ Error processing file {i}: {e}")
                continue

        # ------------------------------------------------------------------- #
        # Combine into Dask DataFrame
        # ------------------------------------------------------------------- #
        if saved_files:
            print(f"\n🔗 Building Dask DataFrame for {table.name}...")
            pattern = str(table_dir / f"{table.name}_part_*.csv")
            ddf = dd.read_csv(pattern)
            print(f"✅ Dask DataFrame ready: {len(ddf.columns)} columns")

            # Optional sample
            print("📊 Sample rows:")
            print(ddf.head())

            # Write combined CSV
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            merged_path = table_dir / f"{table.name}_combined_{timestamp}.csv"
            print("\n💾 Writing combined CSV (lazy compute)...")
            ddf.to_csv(merged_path, index=False, single_file=True)
            print(f"✅ Combined CSV written → {merged_path}")

            return ddf

        else:
            print("⚠️  No files were successfully processed.")
            return None

    except Exception as e:
        print(f"❌ Failed to process table {table_name}: {e}")
        return None


# --------------------------------------------------------------------------- #
# Main Execution Function
# --------------------------------------------------------------------------- #
def main():
    print("🎯 DELTA SHARING - UNIVERSAL FETCH SCRIPT (CSV MODE)")
    print("=" * 80)
    print(f"💾 Memory before start: {get_memory_usage():.1f} MB")

    credential_path = "credentials/config.share"
    client = delta_sharing.SharingClient(credential_path)

    # List all available tables
    tables = list(client.list_all_tables())
    print(f"📚 Total tables available: {len(tables)}")

    if not tables:
        print("⚠️  No tables found in endpoint.")
        return

    # Directory to save everything
    base_dir = RAW_DIR / "delta_sharing_exports_csv"
    base_dir.mkdir(parents=True, exist_ok=True)

    successful_tables = []

    for i, table in enumerate(tables, start=1):
        print(f"\n{'=' * 80}")
        print(f"🚀 Processing Table {i}/{len(tables)}: {table.name}")
        print(f"{'=' * 80}")

        ddf = fetch_table_files(client, table, base_dir)
        if ddf is not None:
            successful_tables.append(table.name)
            gc.collect()

        print(f"\n⏳ Waiting 3 seconds before next table...")
        time.sleep(3)

    print("\n" + "=" * 80)
    print("📋 FINAL SUMMARY")
    print("=" * 80)
    print(f"✅ Tables processed successfully: {len(successful_tables)} / {len(tables)}")
    for t in successful_tables:
        print(f"   • {t}")
    print("=" * 80)
    print(f"💾 Memory after: {get_memory_usage():.1f} MB")
    print("✅ Done.")


if __name__ == "__main__":
    main()

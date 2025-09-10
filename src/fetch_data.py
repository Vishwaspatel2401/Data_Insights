import delta_sharing
import pandas as pd
from datetime import datetime
from src.config import CREDENTIAL_FILE, RAW_DIR   # ⬅️ use config.py

def list_tables():
    """
    List all tables available from Delta Sharing provider.
    Following Databricks documentation pattern exactly.
    """
    # Documentation pattern: client = delta_sharing.SharingClient(f"<profile-path>/config.share")
    client = delta_sharing.SharingClient(f"{CREDENTIAL_FILE}")
    return client.list_all_tables()

def fetch_table(share_name: str, schema_name: str, table_name: str, save: bool = True) -> pd.DataFrame:
    """
    Fetch a specific shared table as a Pandas DataFrame.
    Following Databricks documentation pattern exactly.
    """
    # Documentation pattern: delta_sharing.load_as_pandas(f"<profile-path>#<share-name>.<schema-name>.<table-name>")
    table_path = f"{CREDENTIAL_FILE}#{share_name}.{schema_name}.{table_name}"
    df = delta_sharing.load_as_pandas(table_path)

    if save:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_file = RAW_DIR / f"{table_name}_{timestamp}.parquet"
        df.to_parquet(out_file, index=False)
        print(f"✅ Saved raw data → {out_file}")

    return df

def fetch_all_tables(save: bool = True) -> dict:
    """
    Fetch all shared tables and return them as a dictionary of DataFrames.
    Following Databricks documentation pattern exactly.
    """
    # Documentation pattern: client = delta_sharing.SharingClient(f"<profile-path>/config.share")
    client = delta_sharing.SharingClient(f"{CREDENTIAL_FILE}")
    tables = client.list_all_tables()
    
    dfs = {}
    for t in tables:
        key = f"{t.share}.{t.schema}.{t.name}"
        print(f"📥 Fetching {key} ...")
        df = fetch_table(t.share, t.schema, t.name, save=save)
        dfs[key] = df
    
    return dfs

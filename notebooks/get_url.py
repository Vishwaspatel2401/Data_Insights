import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import delta_sharing

def get_table_files(share, schema, table):
    credential_path = 'credentials/config.share'
    client = delta_sharing.SharingClient(credential_path)
    
    tables = client.list_all_tables()
    target_table = None
    for t in tables:
        if t.share == share and t.schema == schema and t.name == table:
            target_table = t
            break
    
    if not target_table:
        raise Exception(f"Table {share}.{schema}.{table} not found.")
        
    response = client._rest_client.list_files_in_table(target_table)
    
    if response.add_files:
        return response.add_files

try:
    files = get_table_files("payments_prod", "tables", "payments_item_summary")
    if files:
        print(files[0].url)
except Exception as e:
    print(f"Error: {e}")
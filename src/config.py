import os
from pathlib import Path

# -------------------------
# Base project directories
# -------------------------

# Root of the project (dining-analytics/)
BASE_DIR = Path(__file__).resolve().parent.parent

# Data directories
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"

# Credentials
CREDENTIAL_FILE = BASE_DIR / "credentials" / "config.share"

# -------------------------
# Ensure directories exist
# -------------------------
for directory in [RAW_DIR, INTERIM_DIR, PROCESSED_DIR]:
    os.makedirs(directory, exist_ok=True)

import pandas as pd
import os
import glob
import re
from config import MIGRATION_REPORT_DIR

def safe_str(val):
    if val is None: return ""
    try:
        if pd.isna(val): return ""
    except (ValueError, TypeError):
        pass
    return str(val).strip()

def to_camel_case(snake_str):
    s = safe_str(snake_str)
    if not s: return ""
    components = s.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def to_snake_case(str_val):
    """Converts string to snake_case (e.g. FirstName -> first_name)"""
    s = safe_str(str_val)
    if not s: return ""
    # Replace non-alphanumeric with underscore
    s = re.sub(r'[\W\s]+', '_', s)
    # Insert underscore between camelCase
    s = re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()
    # Clean up multiple underscores
    s = re.sub(r'_{2,}', '_', s)
    return s.strip('_')

def get_report_folders():
    if not os.path.exists(MIGRATION_REPORT_DIR): return []
    folders = [f for f in glob.glob(os.path.join(MIGRATION_REPORT_DIR, "*")) if os.path.isdir(f)]
    folders.sort(reverse=True)
    return folders


def format_row_count(n: int) -> str:
    """Return a human-readable row count string e.g. 1234 → '1,234 rows'."""
    return f"{n:,} rows"


def safe_filename(name: str) -> str:
    """Sanitise a string for use in file/directory names."""
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in name)


def resolve_dbname(display_name: str, datasources_df) -> str:
    """
    Convert a datasource display name to its actual dbname.
    Falls back to display_name if not found.

    Args:
        display_name: The human-readable name shown in the UI selectbox.
        datasources_df: DataFrame with columns ['name', 'dbname', ...] from db.get_datasources().
    """
    if not display_name or datasources_df is None or datasources_df.empty:
        return display_name
    match = datasources_df[datasources_df["name"] == display_name]
    if not match.empty:
        return match.iloc[0].get("dbname", display_name)
    return display_name
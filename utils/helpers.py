import pandas as pd
import os
import glob
from config import MIGRATION_REPORT_DIR

def safe_str(val):
    if pd.isna(val) or val is None: return ""
    return str(val).strip()

def to_camel_case(snake_str):
    s = safe_str(snake_str)
    if not s: return ""
    components = s.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def get_report_folders():
    if not os.path.exists(MIGRATION_REPORT_DIR): return []
    folders = glob.glob(os.path.join(MIGRATION_REPORT_DIR, "*"))
    folders.sort(reverse=True)
    return folders
import os

# --- PATHS (ส่วนที่น่าจะขาดหายไป) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ANALYSIS_DIR = os.path.join(BASE_DIR, "analysis_report")
MIGRATION_REPORT_DIR = os.path.join(ANALYSIS_DIR, "migration_report")
DB_FILE = os.path.join(BASE_DIR, "migration_tool.db")

# --- OPTIONS ---
TRANSFORMER_OPTIONS = [
    "TRIM", "UPPER_TRIM", "LOWER_TRIM",
    "BUDDHIST_TO_ISO", "ENG_DATE_TO_ISO",
    "SPLIT_THAI_NAME", "SPLIT_ENG_NAME",
    "FORMAT_PHONE", "MAP_GENDER",
    "TO_NUMBER", "CLEAN_SPACES",
    "REMOVE_PREFIX", "REPLACE_EMPTY_WITH_NULL",
    "LOOKUP_VISIT_ID", "LOOKUP_PATIENT_ID", "LOOKUP_DOCTOR_ID",
    "FLOAT_TO_INT", "PARSE_JSON", "POSTGRES_BIT_CAST"
]

VALIDATOR_OPTIONS = [
    "REQUIRED", "THAI_ID", "HN_FORMAT", 
    "VALID_DATE", "POSITIVE_NUMBER", "IS_EMAIL", "IS_PHONE",
    "NOT_EMPTY", "MIN_LENGTH_13", "NUMERIC_ONLY"
]

DB_TYPES = ["MySQL", "Microsoft SQL Server", "PostgreSQL"]
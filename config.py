import os
from dotenv import load_dotenv

load_dotenv()

# --- PATHS (ส่วนที่น่าจะขาดหายไป) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ANALYSIS_DIR = os.path.join(BASE_DIR, "analysis_report")
MIGRATION_REPORT_DIR = os.path.join(ANALYSIS_DIR, "migration_report")

# --- DATABASE CONFIGURATION ---
def get_database_url() -> str:
    """
    Get PostgreSQL database URL from environment variable.

    Required environment variable:
        DATABASE_URL=postgresql://user:password@localhost:5432/his_analyzer

    Returns:
        str: PostgreSQL connection URL

    Raises:
        RuntimeError: If DATABASE_URL is not set
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL environment variable is required.\n"
            "Example: DATABASE_URL=postgresql://user:password@localhost:5432/his_analyzer\n"
            "Create a .env file with your DATABASE_URL or export it as an environment variable."
        )
    return url

# --- OPTIONS (Dynamic - loaded from registries) ---
def get_transformer_options() -> list[dict]:
    """Get transformer options from registry."""
    from data_transformers.registry import get_transformer_options
    return get_transformer_options()


def get_validator_options() -> list[dict]:
    """Get validator options from registry."""
    from validators.registry import get_validator_options
    return get_validator_options()


def get_db_types() -> list[str]:
    """Get available database types from dialect registry."""
    from dialects.registry import available_types
    return available_types()


# --- LEGACY: Static lists (deprecated, use functions above) ---
TRANSFORMER_OPTIONS = [
    "TRIM", "UPPER_TRIM", "LOWER_TRIM",
    "BUDDHIST_TO_ISO", "ENG_DATE_TO_ISO",
    "SPLIT_THAI_NAME", "SPLIT_ENG_NAME",
    "FORMAT_PHONE", "MAP_GENDER", "VALUE_MAP",
    "TO_NUMBER", "CLEAN_SPACES",
    "REMOVE_PREFIX", "REPLACE_EMPTY_WITH_NULL",
    "GENERATE_HN",
    "LOOKUP_VISIT_ID", "LOOKUP_PATIENT_ID", "LOOKUP_DOCTOR_ID",
    "FLOAT_TO_INT", "PARSE_JSON",
    "BIT_CAST"
]

VALIDATOR_OPTIONS = [
    "REQUIRED", "THAI_ID", "HN_FORMAT",
    "VALID_DATE", "POSITIVE_NUMBER", "IS_EMAIL", "IS_PHONE",
    "NOT_EMPTY", "MIN_LENGTH_13", "NUMERIC_ONLY"
]

from models.db_type import DbType  # noqa: E402 — kept at bottom for legacy compat
DB_TYPES = [DbType.MYSQL, DbType.MSSQL, DbType.POSTGRESQL]
import pandas as pd
import numpy as np
import re
import random
from datetime import datetime
from typing import Any, Dict, List, Optional

class DataTransformer:
    """
    Service for handling data transformations in the ETL pipeline.
    Optimized for Pandas Series (Batch Processing) but supports single value transformation.
    """
    _hn_counter = 0  # Counter for sequential HN generation

    @staticmethod
    def apply_transformers_to_batch(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """
        Main Entry Point: Apply transformers to an entire DataFrame based on config.
        Uses vectorized operations where possible for maximum speed.
        """
        if df.empty or not config or 'mappings' not in config:
            return df

        # Get list of columns present in the dataframe
        available_cols = set(df.columns)

        for mapping in config.get('mappings', []):
            source_col = mapping.get('source')
            target_col = mapping.get('target', source_col)
            transformers = mapping.get('transformers', [])
            transformer_params = mapping.get('transformer_params', {})
            default_value = mapping.get('default_value', None)

            # Special handling for GENERATE_HN: Create column even if source doesn't exist
            if 'GENERATE_HN' in transformers:
                # Create a dummy series with the same length as df for GENERATE_HN
                series_data = pd.Series([None] * len(df), index=df.index)

                for t_name in transformers:
                    try:
                        if t_name == 'VALUE_MAP':
                            vmap_params = transformer_params.get('VALUE_MAP', {})
                            df = DataTransformer.apply_value_map(df, source_col, target_col, vmap_params)
                        else:
                            series_data = DataTransformer.transform_series(series_data, t_name, transformer_params)
                    except Exception as e:
                        print(f"Error transforming {source_col} with {t_name}: {e}")

                df[target_col] = series_data
                continue

            # Skip if source column doesn't exist
            if source_col not in available_cols:
                continue

            # Apply each transformer in sequence
            if transformers:
                # If target is different, copy source to target first (or rename later)
                # Here we operate on source_col and rename at the end of the loop if needed
                series_data = df[source_col]

                for t_name in transformers:
                    try:
                        if t_name == 'VALUE_MAP':
                            vmap_params = transformer_params.get('VALUE_MAP', {})
                            df = DataTransformer.apply_value_map(df, source_col, target_col, vmap_params)
                            series_data = df[target_col] if target_col in df.columns else df[source_col]
                        else:
                            series_data = DataTransformer.transform_series(series_data, t_name, transformer_params)
                    except Exception as e:
                        # Log error but don't crash the whole batch
                        print(f"Error transforming {source_col} with {t_name}: {e}")

                # Assign back to DataFrame
                # If renaming is needed (Source != Target)
                if source_col != target_col:
                    df[target_col] = series_data
                else:
                    df[source_col] = series_data

            # Apply default_value to fill nulls in the result column
            if default_value is not None:
                col_to_fill = target_col if target_col in df.columns else source_col
                if col_to_fill in df.columns:
                    df[col_to_fill] = df[col_to_fill].fillna(default_value)
                    # Also replace empty strings with default_value
                    df[col_to_fill] = df[col_to_fill].replace('', default_value)

        return df

    @staticmethod
    def transform_series(series: pd.Series, transformer_name: str, transformer_params: dict = None) -> pd.Series:
        """
        Apply transformation to a Pandas Series using Vectorized operations.
        transformer_params: Dict with all transformer parameters (may include DEFAULT_VALUE for fallback)
        """
        if transformer_params is None:
            transformer_params = {}
        if series.empty:
            return series

        # --- 1. Fast Vectorized Operations (String/Native Pandas) ---
        # NOTE: Use series.where(series.isna(), ...) to preserve NaN/None.
        # series.astype(str) converts NaN → "nan" which corrupts DB NULL values.
        if transformer_name == "TRIM":
            return series.where(series.isna(), series.astype(str).str.strip())

        if transformer_name == "UPPER_TRIM":
            return series.where(series.isna(), series.astype(str).str.strip().str.upper())

        if transformer_name == "LOWER_TRIM":
            return series.where(series.isna(), series.astype(str).str.strip().str.lower())

        if transformer_name == "CLEAN_SPACES":
            return series.where(series.isna(), series.astype(str).str.replace(r'\s+', ' ', regex=True).str.strip())

        if transformer_name == "TO_NUMBER":
            return series.where(series.isna(), series.astype(str).str.replace(r'\D', '', regex=True))

        if transformer_name == "REPLACE_EMPTY_WITH_NULL":
            return series.where(series.notna() & series.astype(str).str.strip().ne(''), other=np.nan)

        if transformer_name == "DEFAULT_VALUE":
            default_val = transformer_params.get('DEFAULT_VALUE', {}).get('value', None)
            def fill_null(val):
                if pd.isna(val) or val == '':
                    return default_val
                return val
            return series.apply(fill_null)

        if transformer_name == "GENERATE_HN":
            # Generate sequential HN numbers for the entire series
            start_counter = DataTransformer._hn_counter
            result = pd.Series([f"HN{str(i).zfill(9)}" for i in range(start_counter + 1, start_counter + len(series) + 1)], index=series.index)
            DataTransformer._hn_counter += len(series)
            return result

        # --- 2. Complex/Custom Logic (Apply per row) ---
        # These are slower but necessary for complex logic
        complex_transformers = [
            "REMOVE_PREFIX", 
            "BUDDHIST_TO_ISO", 
            "ENG_DATE_TO_ISO", 
            "MAP_GENDER",
            "FORMAT_PHONE",
            "EXTRACT_FIRST_NAME", # Renamed for clarity
            "EXTRACT_LAST_NAME"   # Renamed for clarity
        ]
        
        if transformer_name in complex_transformers:
            return series.apply(lambda x: DataTransformer.transform_value(x, transformer_name, transformer_params))
            
        return series

    @staticmethod
    def transform_value(value: Any, transformer_name: str, transformer_params: dict = None) -> Any:
        """
        Apply transformer to a single scalar value.
        Used as a fallback or for row-by-row processing.
        transformer_params: Dict with all transformer parameters (may include DEFAULT_VALUE for fallback)
        """
        if transformer_params is None:
            transformer_params = {}

        if value is None or pd.isna(value):
            # Fallback chain for null values:
            # 1. Use default_value from specific transformer params
            # 2. Fall back to DEFAULT_VALUE if available
            # 3. Return None
            if transformer_name in transformer_params and 'default_value' in transformer_params[transformer_name]:
                return transformer_params[transformer_name]['default_value']
            if 'DEFAULT_VALUE' in transformer_params:
                return transformer_params['DEFAULT_VALUE'].get('value', None)
            return None
        
        value_str = str(value)

        # Basic text ops
        if transformer_name == "TRIM": return value_str.strip()
        if transformer_name == "UPPER_TRIM": return value_str.strip().upper()
        if transformer_name == "LOWER_TRIM": return value_str.strip().lower()
        if transformer_name == "CLEAN_SPACES": return re.sub(r'\s+', ' ', value_str).strip()
        if transformer_name == "TO_NUMBER": return ''.join(filter(str.isdigit, value_str))
        if transformer_name == "REMOVE_PREFIX": return DataTransformer._remove_prefix(value_str)
        if transformer_name == "REPLACE_EMPTY_WITH_NULL": return None if not value_str.strip() else value_str
        
        # Domain logic
        if transformer_name == "BUDDHIST_TO_ISO": return DataTransformer._buddhist_to_iso(value_str, transformer_params)
        if transformer_name == "ENG_DATE_TO_ISO": return DataTransformer._eng_date_to_iso(value_str)
        if transformer_name == "MAP_GENDER": return DataTransformer._map_gender(value_str)
        if transformer_name == "FORMAT_PHONE": return DataTransformer._format_phone(value_str)
        
        # Name splitting (Map specific parts)
        if transformer_name == "EXTRACT_FIRST_NAME": return DataTransformer._split_name(value_str).get("fname")
        if transformer_name == "EXTRACT_LAST_NAME": return DataTransformer._split_name(value_str).get("lname")
        
        # Generate sequential HN number
        if transformer_name == "GENERATE_HN": return DataTransformer._generate_sequential_hn()
        
        return value

    # --- Internal Helper Methods (Logic Implementation) ---

    @staticmethod
    def _buddhist_to_iso(date_str: str, transformer_params: dict = None) -> Optional[str]:
        """
        Convert Thai Buddhist Date (dd/mm/2566) to ISO
        Falls back to DEFAULT_VALUE if conversion fails
        """
        if transformer_params is None:
            transformer_params = {}

        # Helper to get default value with fallback chain
        def get_default():
            if 'BUDDHIST_TO_ISO' in transformer_params and 'default_value' in transformer_params['BUDDHIST_TO_ISO']:
                return transformer_params['BUDDHIST_TO_ISO']['default_value']
            if 'DEFAULT_VALUE' in transformer_params:
                return transformer_params['DEFAULT_VALUE'].get('value', None)
            return None

        if not date_str or len(date_str) < 8:
            return get_default()

        try:
            # Handle various separators
            parts = re.split(r'[-/]', date_str.strip())
            if len(parts) == 3:
                d, m, y = parts
                # Logic to detect if year is BE (Thailand usually > 2400)
                year_val = int(y)
                # BE years in Thailand are ~2500+; threshold >2400 avoids
                # misidentifying Gregorian years (2001-2399) as Buddhist Era.
                iso_year = year_val - 543 if year_val > 2400 else year_val

                return f"{iso_year}-{m.zfill(2)}-{d.zfill(2)}"
        except:
            pass

        return get_default()

    @staticmethod
    def _eng_date_to_iso(date_str: str) -> Optional[str]:
        """Convert English Date variants to ISO"""
        if not date_str: return None
        try:
            # Try parsing with pandas (very robust)
            return pd.to_datetime(date_str, dayfirst=True).strftime('%Y-%m-%d')
        except:
            pass
            
        # Fallback to manual parsing if pandas fails or is too slow for single value
        try:
            parts = re.split(r'[-/]', date_str.strip())
            if len(parts) == 3:
                d, m, y = parts
                d_val, m_val, y_val = int(d), int(m), int(y)
                if not (1 <= m_val <= 12 and 1 <= d_val <= 31 and y_val > 0):
                    return None
                return f"{y_val:04d}-{m_val:02d}-{d_val:02d}"
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _map_gender(val: str) -> str:
        """Normalize Gender (Thai/Eng) to M/F/U"""
        v = val.strip().lower()
        if v in ['1', 'm', 'male', 'ช', 'ชาย', 'นาย', 'd.b.', 'เด็กชาย']: return 'M'
        if v in ['2', 'f', 'female', 'ญ', 'หญิง', 'นาง', 'นางสาว', 'น.s.', 'ด.ญ.', 'เด็กหญิง']: return 'F'
        return 'U'

    @staticmethod
    def _format_phone(val: str) -> str:
        """Format Thai Phone Number"""
        nums = ''.join(filter(str.isdigit, val))
        if len(nums) == 10 and nums.startswith('0'):
            return f"{nums[:3]}-{nums[3:6]}-{nums[6:]}"
        elif len(nums) == 9 and nums.startswith('0'): # Landline
            return f"{nums[:2]}-{nums[2:5]}-{nums[5:]}"
        return nums

    @staticmethod
    def _remove_prefix(val: str) -> str:
        """Remove common Thai prefixes"""
        prefixes = ['นาย', 'นาง', 'น.ส.', 'นางสาว', 'ด.ช.', 'ด.ญ.', 'เด็กชาย', 'เด็กหญิง', 'Mr.', 'Mrs.', 'Ms.']
        # Sort by length desc to handle 'นางสาว' before 'นาง'
        prefixes.sort(key=len, reverse=True) 
        
        val = val.strip()
        for p in prefixes:
            if val.startswith(p):
                return val[len(p):].strip()
        return val

    @staticmethod
    def _split_name(val: str) -> Dict[str, str]:
        """Split name into First and Last"""
        clean_val = DataTransformer._remove_prefix(val)
        parts = clean_val.split()
        if len(parts) >= 2:
            return {"fname": parts[0], "lname": " ".join(parts[1:])}
        return {"fname": clean_val, "lname": ""}

    @staticmethod
    def _generate_sequential_hn() -> str:
        """Generate sequential HN number (e.g., HN000000001, HN000000002, ...)"""
        DataTransformer._hn_counter += 1
        return f"HN{str(DataTransformer._hn_counter).zfill(9)}"
    
    @staticmethod
    def reset_hn_counter(start_value: int = 0):
        """Reset HN counter to specified value (useful for testing or new migrations)"""
        DataTransformer._hn_counter = start_value

    @staticmethod
    def apply_value_map(df: pd.DataFrame, source_col: str, target_col: str, params: dict) -> pd.DataFrame:
        """
        Apply conditional value mapping to a DataFrame.
        Supports single-column and multi-column conditions.

        params = {
            "rules": [
                {"when": {"Sex": "1"}, "then": "M"},
                {"when": {"Sex": "2"}, "then": "F"},
                {"when": {"type": "A", "grade": "1"}, "then": "PASS"}
            ],
            "default": null or value to use when no rule matches
        }

        If default is None, keeps the source value when no rule matches.
        """
        rules = params.get('rules', [])
        default_val = params.get('default', None)

        if not rules:
            # No rules defined, just copy source to target if they differ
            if source_col != target_col:
                df[target_col] = df[source_col]
            return df

        def map_row(row):
            for rule in rules:
                conditions = rule.get('when', {})
                # Check if all condition columns match their values
                match = True
                for col, val in conditions.items():
                    row_val = str(row.get(col, '')).strip()
                    cond_val = str(val).strip()
                    if row_val != cond_val:
                        match = False
                        break

                if match:
                    return rule.get('then')

            # No rule matched - use default or keep source
            if default_val is not None:
                return default_val
            else:
                # Keep original source value
                return row.get(source_col)

        df[target_col] = df.apply(map_row, axis=1)
        return df
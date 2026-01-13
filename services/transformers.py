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

            # Special handling for GENERATE_HN: Create column even if source doesn't exist
            if 'GENERATE_HN' in transformers:
                # Create a dummy series with the same length as df for GENERATE_HN
                series_data = pd.Series([None] * len(df), index=df.index)

                for t_name in transformers:
                    try:
                        series_data = DataTransformer.transform_series(series_data, t_name)
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
                        series_data = DataTransformer.transform_series(series_data, t_name)
                    except Exception as e:
                        # Log error but don't crash the whole batch
                        print(f"Error transforming {source_col} with {t_name}: {e}")

                # Assign back to DataFrame
                # If renaming is needed (Source != Target)
                if source_col != target_col:
                    df[target_col] = series_data
                else:
                    df[source_col] = series_data

        return df

    @staticmethod
    def transform_series(series: pd.Series, transformer_name: str) -> pd.Series:
        """
        Apply transformation to a Pandas Series using Vectorized operations.
        """
        if series.empty:
            return series

        # --- 1. Fast Vectorized Operations (String/Native Pandas) ---
        if transformer_name == "TRIM":
            return series.astype(str).str.strip()
        
        if transformer_name == "UPPER_TRIM":
            return series.astype(str).str.strip().str.upper()
        
        if transformer_name == "LOWER_TRIM":
            return series.astype(str).str.strip().str.lower()
            
        if transformer_name == "CLEAN_SPACES":
            return series.astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
        
        if transformer_name == "TO_NUMBER":
            return series.astype(str).str.replace(r'\D', '', regex=True)

        if transformer_name == "REPLACE_EMPTY_WITH_NULL":
            return series.replace(r'^\s*$', np.nan, regex=True)

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
            return series.apply(lambda x: DataTransformer.transform_value(x, transformer_name))
            
        return series

    @staticmethod
    def transform_value(value: Any, transformer_name: str) -> Any:
        """
        Apply transformer to a single scalar value.
        Used as a fallback or for row-by-row processing.
        """
        if value is None or pd.isna(value): 
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
        if transformer_name == "BUDDHIST_TO_ISO": return DataTransformer._buddhist_to_iso(value_str)
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
    def _buddhist_to_iso(date_str: str) -> Optional[str]:
        """Convert Thai Buddhist Date (dd/mm/2566) to ISO"""
        if not date_str or len(date_str) < 8: return None
        try:
            # Handle various separators
            parts = re.split(r'[-/]', date_str.strip())
            if len(parts) == 3:
                d, m, y = parts
                # Logic to detect if year is BE (Thailand usually > 2400)
                year_val = int(y)
                iso_year = year_val - 543 if year_val > 2000 else year_val
                
                return f"{iso_year}-{m.zfill(2)}-{d.zfill(2)}"
        except:
            pass
        return None # Return None on failure to ensure DB consistency

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
                return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        except:
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
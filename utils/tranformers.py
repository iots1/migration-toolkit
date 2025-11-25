import re
from datetime import datetime

def apply_transformer(value, transformer_name):
    """Router function to apply specific transformer"""
    if value is None: return None
    value = str(value)
    
    if transformer_name == "TRIM": return value.strip()
    if transformer_name == "UPPER_TRIM": return value.strip().upper()
    if transformer_name == "LOWER_TRIM": return value.strip().lower()
    if transformer_name == "CLEAN_SPACES": return re.sub(r'\s+', ' ', value).strip()
    if transformer_name == "TO_NUMBER": return ''.join(filter(str.isdigit, value))
    if transformer_name == "REMOVE_PREFIX": return remove_prefix(value)
    if transformer_name == "REPLACE_EMPTY_WITH_NULL": return None if not value.strip() else value
    
    # Date Handling
    if transformer_name == "BUDDHIST_TO_ISO": return buddhist_to_iso(value)
    if transformer_name == "ENG_DATE_TO_ISO": return eng_date_to_iso(value)
    
    # Specific Domain
    if transformer_name == "MAP_GENDER": return map_gender(value)
    if transformer_name == "FORMAT_PHONE": return format_phone(value)
    
    # Complex (Might return dict or list, handle with care in UI)
    if transformer_name == "SPLIT_THAI_NAME": return split_name(value)
    
    return value

# --- Implementation Details ---

def buddhist_to_iso(date_str):
    """Convert Thai Buddhist Date (dd/mm/2566) to ISO (2023-mm-dd)"""
    if not date_str or len(date_str) < 8: return None
    try:
        # Assumes format like 31/12/2566 or 31-12-2566
        parts = re.split(r'[-/]', date_str)
        if len(parts) == 3:
            d, m, y = parts
            iso_year = int(y) - 543
            return f"{iso_year}-{m.zfill(2)}-{d.zfill(2)}"
    except:
        pass
    return date_str # Return original on fail

def eng_date_to_iso(date_str):
    """Convert English Date (dd/mm/yyyy) to ISO"""
    if not date_str: return None
    try:
        parts = re.split(r'[-/]', date_str)
        if len(parts) == 3:
            d, m, y = parts
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except:
        pass
    return date_str

def map_gender(val):
    """Normalize Gender"""
    v = val.strip().lower()
    if v in ['1', 'm', 'male', 'ช', 'ชาย']: return 'M'
    if v in ['2', 'f', 'female', 'ญ', 'หญิง']: return 'F'
    return 'U'

def format_phone(val):
    """Format Thai Phone Number"""
    nums = ''.join(filter(str.isdigit, val))
    if len(nums) == 10 and nums.startswith('0'):
        return f"{nums[:3]}-{nums[3:6]}-{nums[6:]}"
    return nums

def split_name(val):
    """Simple name splitter"""
    parts = val.strip().split()
    if len(parts) >= 2:
        return {"fname": parts[0], "lname": " ".join(parts[1:])}
    return {"fname": val, "lname": ""}

def remove_prefix(val):
    prefixes = ['นาย', 'นาง', 'น.ส.', 'นางสาว', 'ด.ช.', 'ด.ญ.', 'Mr.', 'Mrs.', 'Ms.']
    for p in prefixes:
        if val.startswith(p):
            return val[len(p):].strip()
    return val
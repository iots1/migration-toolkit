import re
from datetime import datetime

def validate_value(value, validator_name):
    """Returns (True, "") if valid, (False, "Error Message") if invalid"""
    if value is None: value = ""
    value = str(value).strip()

    if validator_name == "REQUIRED" or validator_name == "NOT_EMPTY":
        return (bool(value), "Value is required")
    
    if not value: return (True, "") # Skip other checks if empty and not required

    if validator_name == "NUMERIC_ONLY":
        return (value.isdigit(), "Must be numeric")
        
    if validator_name == "POSITIVE_NUMBER":
        try:
            return (float(value) > 0, "Must be > 0")
        except:
            return (False, "Not a number")

    if validator_name == "MIN_LENGTH_13":
        return (len(value) >= 13, "Length must be >= 13")

    if validator_name == "IS_EMAIL":
        pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
        return (bool(re.match(pattern, value)), "Invalid Email format")
        
    if validator_name == "IS_PHONE":
        # Simple Thai phone check
        return (len(re.sub(r'\D', '', value)) >= 9, "Invalid Phone format")

    if validator_name == "THAI_ID":
        return (check_thai_id(value), "Invalid Thai ID Checksum")

    if validator_name == "HN_FORMAT":
        # Example HN format checking
        return (len(value) > 0, "Invalid HN") 

    if validator_name == "VALID_DATE":
        # Basic ISO date check
        try:
            datetime.strptime(value, '%Y-%m-%d')
            return (True, "")
        except:
            return (False, "Invalid Date (YYYY-MM-DD)")

    return (True, "")

def check_thai_id(id_number):
    if len(id_number) != 13 or not id_number.isdigit(): return False
    digits = [int(d) for d in id_number]
    checksum = sum((13 - i) * digits[i] for i in range(12)) % 11
    check_digit = (11 - checksum) % 10
    return check_digit == digits[12]
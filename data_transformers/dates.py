"""Date transformers - Date format conversions."""
import pandas as pd
from data_transformers.registry import register_transformer


@register_transformer("BUDDHIST_TO_ISO", "Buddhist to ISO", "Convert Thai BE years to CE", has_params=True)
def buddhist_to_iso(series: pd.Series, params=None) -> pd.Series:
    """
    Convert Thai Buddhist Era (BE) years to ISO (CE).

    BE = CE + 543

    Params:
        source_format: Format of source dates (default: 'mixed')
        target_format: Target date format (default: 'ISO')
    """
    if params is None:
        params = {}

    def convert_year(date_str):
        if pd.isna(date_str) or date_str == '':
            return date_str
        date_str = str(date_str)
        # Extract year (assume 4 digits)
        match = re.search(r'\d{4}', date_str)
        if match:
            year = int(match.group())
            if year > 2400:  # BE year
                new_year = year - 543
                date_str = date_str.replace(str(year), str(new_year))
        return date_str

    import re
    return series.apply(convert_year)


@register_transformer("ENG_DATE_TO_ISO", "English Date to ISO", "Convert various English date formats to ISO", has_params=True)
def eng_date_to_iso(series: pd.Series, params=None) -> pd.Series:
    """
    Convert English date formats to ISO format (YYYY-MM-DD).

    Handles: DD/MM/YYYY, MM/DD/YYYY, etc.
    """
    if params is None:
        params = {}

    def parse_date(date_str):
        if pd.isna(date_str) or date_str == '':
            return date_str
        date_str = str(date_str).strip()
        # Try common formats
        for fmt in ['%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d', '%d-%m-%Y']:
            try:
                parsed = pd.to_datetime(date_str, format=fmt)
                return parsed.strftime('%Y-%m-%d')
            except:
                continue
        return date_str

    return series.apply(parse_date)

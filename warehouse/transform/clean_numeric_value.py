import pandas as pd

def clean_numeric_column(series: pd.Series) -> pd.Series:
    """
    This function removes the $ symbol or non-numeric characters from an entire Pandas Series column.
    It returns a Series with a float data type.
    """
    return series.astype(str).str.replace(r'[^0-9.-]', '', regex=True).astype(float)


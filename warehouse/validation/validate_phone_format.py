import re
import pandas as pd

# Ensure phone number contains 13 digits
def validate_phone_format(phone):
    if phone is None or (isinstance(phone, float) and pd.isna(phone)):  # Cek None atau NaN
        return False  # Anggap nomor tidak valid jika null
    
    phone_regex = re.compile(r"^\d{13}$")
    return bool(phone_regex.match(str(phone)))  # Konversi ke string agar tidak error

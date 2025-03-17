import re

def validate_date_format(date):
    if isinstance(date, str):
        date_str = date  # Jika sudah string, langsung gunakan
    elif hasattr(date, "strftime"):  # Jika date adalah datetime.date atau datetime.datetime
        date_str = date.strftime("%Y-%m-%d")
    else:
        return False  # Jika bukan string atau date, tidak valid

    date_regex = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")
    return bool(date_regex.match(date_str))

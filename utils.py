import re

def load_procedure(path):
    with open(path, 'r') as f:
        return f.read()

def extract_proc_name(sql_code):
    match = re.search(r'(CREATE\s+(OR\s+REPLACE\s+)?(PROCEDURE|VIEW))\s+([\w\.]+)', sql_code, re.IGNORECASE)
    return match.group(4) if match else "UnknownObject"

def detect_object_type(sql_code):
    if "CREATE VIEW" in sql_code.upper():
        return "view"
    elif "CREATE PROCEDURE" in sql_code.upper():
        return "procedure"
    else:
        return "object"

def extract_tables(sql_code):
    tables = set()
    for match in re.finditer(r'(FROM|INTO|UPDATE|JOIN)\s+([\[\]\w\d_\.]+)', sql_code, re.IGNORECASE):
        tables.add(match.group(2))
    return list(tables)

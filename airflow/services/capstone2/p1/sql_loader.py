import os

def load_sql(filename):
    path = os.path.join(os.path.dirname(__file__), 'sql', filename)
    with open(path, 'r', encoding='utf-8') as file:
        return file.read().strip()  # ← strip whitespace & ensure string

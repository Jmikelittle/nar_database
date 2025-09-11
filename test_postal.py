import pandas as pd
from nar_database.database import NARDatabase
from pathlib import Path

# Check original CSV format
csv_path = 'test_data/raw/extracted/Addresses/Address_10.csv'
df = pd.read_csv(csv_path, nrows=20)

print('Original postal codes from CSV (MAIL_POSTAL_CODE):')
postal_codes = df['MAIL_POSTAL_CODE'].dropna().head(5)
for i, code in enumerate(postal_codes, 1):
    print(f'{i:2d}. "{code}" (length: {len(code)})')

# Check what we stored in database
print('\nStored in database:')
db = NARDatabase(Path('test_data/test.db'))
with db.get_connection() as conn:
    cursor = conn.execute('SELECT postal_code FROM addresses WHERE postal_code IS NOT NULL LIMIT 5')
    stored_codes = cursor.fetchall()
    for i, row in enumerate(stored_codes, 1):
        code = row[0]
        print(f'{i:2d}. "{code}" (length: {len(code)})')

from pathlib import Path
from nar_database.processor import NARProcessor
from nar_database.database import NARDatabase

print('🧪 Testing corrected postal code format (no spaces)...')

# Set up with corrected format
processor = NARProcessor(Path('test_data'))
db = NARDatabase(Path('test_data/test_corrected.db'))

print('1. Creating database schema...')
db.create_schema()

addr_file = Path('test_data/raw/extracted/Addresses/Address_10.csv')
print('2. Processing with corrected postal code format...')

record_count = 0
for chunk in processor.process_csv_file(addr_file, chunk_size=100):
    print(f'   Processing {len(chunk)} records...')
    db.insert_addresses_batch(chunk)
    record_count += len(chunk)
    if record_count >= 100:
        break

print(f'✅ Processed {record_count} records with corrected format')

# Check the postal codes now
print('3. Checking postal code format in database:')
with db.get_connection() as conn:
    cursor = conn.execute('SELECT DISTINCT postal_code FROM addresses WHERE postal_code IS NOT NULL LIMIT 5')
    postal_codes = cursor.fetchall()
    for i, row in enumerate(postal_codes, 1):
        code = row[0]
        print(f'   {i}. "{code}" (length: {len(code)})')

# Test query
print('4. Testing query with original CSV format:')
results = db.query_by_postal_code('A1L2H4')  # Original CSV format
print(f'   Query for "A1L2H4" found: {len(results)} results')

results2 = db.query_by_postal_code('A1L 2H4')  # User might type with space
print(f'   Query for "A1L 2H4" found: {len(results2)} results')

print('✅ Corrected format test complete!')

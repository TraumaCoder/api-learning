import requests
import pyodbc
import os

# Constants
BASE_URL = "https://api.fda.gov/drug/label.json"
SEARCH_TERM = 'openfda.substance_name:"ibuprofen"'
LIMIT = 100

# DB connection
server = os.environ.get('DB_SERVER')
database = os.environ.get('DB_NAME')
username = os.environ.get('DB_USER')
password = os.environ.get('DB_PASSWORD')

connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;'

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute('''
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fda_drug_labels' AND xtype='U')
    CREATE TABLE fda_drug_labels (
        id INT IDENTITY(1,1) PRIMARY KEY,
        brand_name VARCHAR(255),
        manufacturer VARCHAR(255),
        product_type VARCHAR(100),
        route VARCHAR(100),
        loaded_date DATETIME DEFAULT GETDATE()
    )
''')
conn.commit()
print("Table ready.")

# Paginated API pull
all_drugs = []
skip = 0
total = None

print("Starting FDA pull...")

while True:
    params = {
        "search": SEARCH_TERM,
        "limit": LIMIT,
        "skip": skip
    }
    
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    
    if total is None:
        total = data['meta']['results']['total']
        print(f"Total records: {total}")
    
    records = data['results']
    for drug in records:
        all_drugs.append((
            drug.get('openfda', {}).get('brand_name', ['N/A'])[0],
            drug.get('openfda', {}).get('manufacturer_name', ['N/A'])[0],
            drug.get('openfda', {}).get('product_type', ['N/A'])[0],
            drug.get('openfda', {}).get('route', ['N/A'])[0]
        ))
    
    print(f"Pulled records {skip + 1} to {skip + len(records)}")
    skip += LIMIT
    
    if skip >= total:
        break

print(f"API pull complete. {len(all_drugs)} records.")

# Insert into SQL Server
cursor.executemany('''
    INSERT INTO fda_drug_labels (brand_name, manufacturer, product_type, route)
    VALUES (?, ?, ?, ?)
''', all_drugs)

conn.commit()
conn.close()

print(f"Done. {len(all_drugs)} records loaded into fda_drug_labels.")
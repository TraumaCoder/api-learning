import pyodbc
import os

server = os.environ.get('DB_SERVER')
database = os.environ.get('DB_NAME')
username = os.environ.get('DB_USER')
password = os.environ.get('DB_PASSWORD')

connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;'

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Create a test table
cursor.execute('''
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='test_table' AND xtype='U')
    CREATE TABLE test_table (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name VARCHAR(100),
        created_date DATETIME DEFAULT GETDATE()
    )
''')

# Insert a row
cursor.execute("INSERT INTO test_table (name) VALUES (?)", ('TraumaCoder',))
conn.commit()

# Read it back
cursor.execute("SELECT * FROM test_table")
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()
print("Done!")
import requests
import pyodbc
import os
import time
import logging
from datetime import datetime


#logging setup
logging.basicConfig(
    filename=f'fda_load_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#constants
BASE_URL = "https://api.fda.gov/drug/label.json"
SEARCH_TERM = 'openfda.substance_name:"ibuprofen"'
LIMIT = 100
MAX_RETRIES = 3
RETRY_DELAY = 1 #seconds, doubles each retry 

#retry engine

def call_api_with_retry(url, params):
    retries = 0
    delay = RETRY_DELAY
    
    while retries < MAX_RETRIES:
        try:
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            else:
                logging.warning(f"API returned {response.status_code} on attempt {retries + 1}")
        
        except requests.exceptions.RequestException as e:
            logging.warning(f"API call failed on attempt {retries + 1}: {e}")
        
        retries += 1
        if retries < MAX_RETRIES:
            logging.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 2  # exponential backoff - doubles each retry
    
    logging.error(f"API call failed after {MAX_RETRIES} attempts")
    return None


# record extraction function 
def extract_record(drug):
    try:
        return {
            "brand_name": drug.get('openfda', {}).get('brand_name', ['N/A'])[0],
            "manufacturer": drug.get('openfda', {}).get('manufacturer_name', ['N/A'])[0],
            "product_type": drug.get('openfda', {}).get('product_type', ['N/A'])[0],
            "route": drug.get('openfda', {}).get('route', ['N/A'])[0]
        }
    except Exception as e:
        logging.warning(f"Skipping bad record: {e} | Raw data: {drug.get('openfda', {})}")
        return None

# Main pipeline function
def run_pipeline():
    # DB connection
    server = os.environ.get('DB_SERVER')
    database = os.environ.get('DB_NAME')
    username = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')

    connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;Connection Timeout=30;'

    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        logging.info("Database connected successfully")
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return

    # Create table if not exists
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fda_drug_labels_safe' AND xtype='U')
        CREATE TABLE fda_drug_labels_safe (
            id INT IDENTITY(1,1) PRIMARY KEY,
            brand_name VARCHAR(255),
            manufacturer VARCHAR(255),
            product_type VARCHAR(100),
            route VARCHAR(100),
            loaded_date DATETIME DEFAULT GETDATE()
        )
    ''')
    conn.commit()
    logging.info("Table ready")

    # Paginated API pull
    all_drugs = []
    skipped = 0
    skip = 0
    total = None

    logging.info("Starting FDA pull...")
    print("Starting FDA pull...")

    while True:
        params = {
            "search": SEARCH_TERM,
            "limit": LIMIT,
            "skip": skip
        }

        data = call_api_with_retry(BASE_URL, params)

        if data is None:
            logging.error(f"Pipeline stopped at skip={skip} due to API failure")
            print("API failed after retries — check log for details")
            conn.close()
            return

        if total is None:
            total = data['meta']['results']['total']
            logging.info(f"Total records: {total}")
            print(f"Total records: {total}")

        for drug in data['results']:
            record = extract_record(drug)
            if record is not None:
                all_drugs.append((
                    record['brand_name'],
                    record['manufacturer'],
                    record['product_type'],
                    record['route']
                ))
            else:
                skipped += 1

        print(f"Pulled records {skip + 1} to {skip + len(data['results'])}")
        logging.info(f"Pulled records {skip + 1} to {skip + len(data['results'])}")
        skip += LIMIT

        if skip >= total:
            break

    logging.info(f"API pull complete. {len(all_drugs)} records pulled, {skipped} skipped")
    print(f"API pull complete. {len(all_drugs)} records, {skipped} skipped")

    # Insert with transaction - roll back if anything fails
    try:
        cursor.executemany('''
            INSERT INTO fda_drug_labels_safe (brand_name, manufacturer, product_type, route)
            VALUES (?, ?, ?, ?)
        ''', all_drugs)
        conn.commit()
        logging.info(f"Successfully loaded {len(all_drugs)} records")
        print(f"Done. {len(all_drugs)} records loaded into fda_drug_labels_safe")
    except Exception as e:
        conn.rollback()
        logging.error(f"Insert failed, transaction rolled back: {e}")
        print(f"Insert failed - rolled back. Check log for details")
    finally:
        conn.close()
        logging.info("Connection closed")

# Run it
run_pipeline()

print("Script finished")
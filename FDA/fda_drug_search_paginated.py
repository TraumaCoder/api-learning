import requests

# Constants
BASE_URL = "https://api.fda.gov/drug/label.json"
SEARCH_TERM = 'openfda.substance_name:"ibuprofen"'
LIMIT = 100  # records per page

# Variables
all_drugs = []
skip = 0
total = None

print("Starting FDA drug label pull...")

# Pagination loop
while True:
    params = {
        "search": SEARCH_TERM,
        "limit": LIMIT,
        "skip": skip
    }
    
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    
    # First pass - grab total
    if total is None:
        total = data['meta']['results']['total']
        print(f"Total records found: {total}")
    
    # Extract records from this page
    records = data['results']
    for drug in records:
        all_drugs.append({
            "brand_name": drug.get('openfda', {}).get('brand_name', ['N/A'])[0],
            "manufacturer": drug.get('openfda', {}).get('manufacturer_name', ['N/A'])[0],
            "product_type": drug.get('openfda', {}).get('product_type', ['N/A'])[0],
            "route": drug.get('openfda', {}).get('route', ['N/A'])[0]
        })
    
    print(f"Pulled records {skip + 1} to {skip + len(records)}")
    
    # Advance to next page
    skip += LIMIT
    
    # Stop when we've pulled everything
    if skip >= total:
        break

print(f"---")
print(f"Done. Total records pulled: {len(all_drugs)}")
print(f"First record: {all_drugs[0]}")
print(f"Last record: {all_drugs[-1]}")
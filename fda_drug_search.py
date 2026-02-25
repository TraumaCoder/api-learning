import requests

#Base URL and parameters 
url = "https://api.fda.gov/drug/label.json"
params = {
    "search": 'openfda.substance_name:"ibuprofen"',
    "limit": 5
}

#Make the call
response = requests.get(url, params=params)
print(response.json())
#Parse the response
data = response.json()

#print status and total results
print(f"Status Code: {response.status_code}")
print(f"Total Records: {data['meta']['results']['total']}")
print("---")

#loop through results and print key fields 
for drug in data['results']:
    print(f"Brand Name: {drug.get('openfda', {}).get('brand_name', ['N/A'])[0]}")
    print(f"Manufacturer: {drug.get('openfda', {}).get('manufacturer_name', ['N/A'])[0]}")
    print("---")
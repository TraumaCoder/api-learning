import requests

response = requests.get("https://api.agify.io/?name=will")

data = response.json()

print(f"Status Code: {response.status_code}")
print(f"Name: {data['name']}")
print(f"Predicted Age: {data['age']}")
print(f"Sample Size: {data['count']}")
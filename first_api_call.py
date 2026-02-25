import requests

response = requests.get("https://api.agify.io/?name=will")
                        
print(response.status_code) 
print(response.json()) 
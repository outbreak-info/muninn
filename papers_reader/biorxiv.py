import requests

url = "https://api.biorxiv.org/details/biorxiv/2024-01-01/2024-01-31/0/json"

response = requests.get(url)

data = response.json()

l
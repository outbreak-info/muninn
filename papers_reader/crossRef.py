import requests
from time import sleep

def get_doi_from_crossref(title):
    url = "https://api.crossref.org/works"
    params = {"query": title, "rows": 1}
    
    response = requests.get(url, params=params)
    sleep(0.2)
    if response.status_code == 200:
        items = response.json().get("message", {}).get("items", [])
        if items:
            return items[0].get("DOI")
    return None
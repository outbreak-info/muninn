import requests
from collections import Counter
import argparse
from json import dump

parser = argparse.ArgumentParser()
parser.add_argument("-fout")
args = parser.parse_args()


base_url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
query = "H5N1 AND SRC:PPR"
page_size = 100
cursor = "*"

all_results = []

while True:
    params = {
        "query": query,
        "format": "json",
        "pageSize": page_size,
        "cursorMark": cursor
    }

    response = requests.get(base_url, params=params)
    data = response.json()

    results = data.get("resultList", {}).get("result", [])
    all_results.extend(results)

    next_cursor = data.get("nextCursorMark")
    if not next_cursor or next_cursor == cursor:
        break  # No more results

    cursor = next_cursor

print(f'Hits: {len(all_results)}')

with open(args.fout, 'w') as fout:
    dump(all_results,fout)
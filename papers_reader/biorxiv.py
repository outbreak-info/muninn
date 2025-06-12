import requests
from papers_reader.crossRef import get_doi_from_crossref

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

debug_log = {
    'doi_found': 0,
    'doi_not_found': 0,
    'doi_recovered': 0
}


for result in all_results:
    doi = result.get('doi')
    if not doi:
        title = result.get('title', '')
        recovered_doi = get_doi_from_crossref(title)
        if recovered_doi:
            print(f"Recovered DOI: {recovered_doi} for title: {title}")
            debug_log['doi_recovered'] += 1
        else:
            print(f"DOI not found for title: {title}")
            debug_log['doi_not_found'] += 1
    else:
        debug_log['doi_found'] += 1

print(debug_log)
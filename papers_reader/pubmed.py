from typing import List
import requests
from itertools import islice
from crossRef import get_doi_from_crossref
from collections import Counter


def chunks(iterable, size):
    iterable = iter(iterable)
    return iter(lambda: list(islice(iterable, size)), [])

base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
query = "H5N1"

params = {
    "term": query,
    "retmode": "json",
    "retmax": 10000,
}

response = requests.get(base_url, params=params)
data: dict = response.json()
ids = data.get("esearchresult",{}).get("idlist",[])

summary_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

for batch in chunks(ids,200):
    id_string = ",".join(batch)
    params = {
    "db": "pubmed",
    "id": id_string,
    "retmode": "json",
    }

response = requests.get(summary_url, params=params)
data: dict = response.json().get("result",{})

idtypes = Counter()

dataIter = iter(data.items())
next(dataIter)
for id, content in dataIter:
    articleId: List[dict] = content.get('articleids')
    if not articleId:
        debug_log['aid_not_found'] += 1
        continue
    title = content.get('title')
    doi = None
    for id in articleId:
        idtypes[id.get('idtype')] += 1
    if True:
        continue
    recovered_doi = get_doi_from_crossref(title)
    if recovered_doi:
        debug_log['doi_recovered'] += 1
    else:
        print(title)
        debug_log['doi_not_found'] += 1

print(idtypes)

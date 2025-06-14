import requests
from crossRef import get_doi_from_crossref
from collections import Counter
from json import load
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-fin")
parser.add_argument("-n")
args = parser.parse_args()

openAccess = Counter()

with open(args.fin, 'r') as fin:
    results = load(fin)

start = 20*int(args.n)
end = start+20

for result in results[start:end]:
    doi = result.get('doi')
    ft = requests.get(f'https://doi.org/{doi}')
    openAccess[ft.status_code] += 1
    if ft.status_code == 200:
        print(doi)

print(openAccess)
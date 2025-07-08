from csv import DictReader, DictWriter

ref2gff = {
    'HA1-5':'HA:cds-XAJ25415.1',
    'M1':'M1:cds-XAJ25416.1',
    'M2':'M2:cds-XAJ25417.1',
    'NA-1':'NA:cds-XAJ25418.1',
    'NEP':'NEP:cds-XAJ25421.1',
    'NP':'NP:cds-XAJ25419.1',
    'NS-1':'NS1:cds-XAJ25420.1',
    'PA':'PA:cds-XAJ25422.1',
    'PA-X':'PA-X:cds-XAJ25423.1',
    'PB1':'PB1:cds-XAJ25424.1',
    'PB1-F2':'PB1-F2:cds-XAJ25425.1',
    'PB2':'PB2:cds-XAJ25426.1'
}

with open('flumut_annotations.csv') as fin:
    csvreader = DictReader(fin)
    print(next(csvreader))



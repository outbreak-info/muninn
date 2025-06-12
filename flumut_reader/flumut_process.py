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

with open('flumut_annotations.csv') as fin, open('flumut_reader/flumut_annotations_processed.csv', 'w') as fout:
    csvreader = DictReader(fin)
    rowKeys = ['gff_feature','ref_aa','position_aa','alt_aa','publication_year','author','detail','doi']
    writer = DictWriter(fout, fieldnames=rowKeys)
    writer.writeheader()
    for line in csvreader:
        mutation_name = line['mutation_name']
        paper_id = line['id']
        doi = line['doi']
        effect_name = line['effect_name']
        publication_year = paper_id[-4:]
        author = paper_id[:-5]
        if paper_id[-1] == 'b':
            publication_year = paper_id[-5:-1]
            author = paper_id[:-7]
        split_mutation = mutation_name.split(':')
        ref = split_mutation[0]
        mutation = split_mutation[1]
        if ref not in ref2gff.keys():
            continue
        values = [ref2gff[split_mutation[0]],mutation[0],mutation[1:-1],mutation[-1],publication_year,author,effect_name,doi]
        writer.writerow(dict(zip(rowKeys,values)))



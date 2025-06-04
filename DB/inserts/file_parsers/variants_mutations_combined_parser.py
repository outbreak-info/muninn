from DB.inserts.file_parsers.file_parser import FileParser


class VariantsMutationsCombinedParser(FileParser):

    def __init__(self, variants_filename: str, mutations_filename: str):
        self.variants_filename = variants_filename
        self.mutations_filename = mutations_filename

    def parse_and_insert(self):
        #  1. read vars and muts
        #  2. Get accession -> id mapping from db
        #  3. Filter out vars and muts with accessions missing from db
        #  4. split out and combine alleles
        #  5. filter out existing alleles
        #  6. Insert new alleles via copy
        #  7. split out and combine amino acid subs
        #  8. filter out existing AA subs
        #  9. insert new aa subs via copy
        # 10. Get new allele / AAS ids and join back into vars and muts
        # 11. Split out and insert new translations from vars and muts
        # 12. Filter out existing mutations (updates not allowed)
        # 13. insert new mutations via copy
        # 14. Separate new and existing variants
        # 15. Insert new variants via copy
        # 16. Update existing variants (new bulk process for this?)
        pass




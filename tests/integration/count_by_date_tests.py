import unittest

import requests


class CountByDateTests(unittest.TestCase):

    def test_count_samples(self):
        res = requests.get('http://localhost:8000/v0/samples:count?group_by=release_date')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/samples:count?group_by=creation_date&date_bin=week')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/samples:count?group_by=creation_date&date_bin=day')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/samples:count?group_by=collection_date')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/samples:count?group_by=collection_date&date_bin=week')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/samples:count?group_by=collection_date&date_bin=day')
        self.assertTrue(res.ok)

    def test_count_variants(self):
        res = requests.get('http://localhost:8000/v0/variants:count?group_by=release_date')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/variants:count?group_by=creation_date&date_bin=week')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/variants:count?group_by=creation_date&date_bin=day')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/variants:count?group_by=collection_date')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/variants:count?group_by=collection_date&date_bin=week')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/variants:count?group_by=collection_date&date_bin=day')
        self.assertTrue(res.ok)

    def test_count_mutations(self):
        res = requests.get('http://localhost:8000/v0/mutations:count?group_by=release_date')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/mutations:count?group_by=creation_date&date_bin=week')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/mutations:count?group_by=creation_date&date_bin=day')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/mutations:count?group_by=collection_date')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/mutations:count?group_by=collection_date&date_bin=week')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/mutations:count?group_by=collection_date&date_bin=day')
        self.assertTrue(res.ok)

    def test_count_lineages(self):
        res = requests.get('http://localhost:8000/v0/lineages:count?group_by=release_date')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/lineages:count?group_by=creation_date&date_bin=week')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/lineages:count?group_by=creation_date&date_bin=day')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/lineages:count?group_by=collection_date')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/lineages:count?group_by=collection_date&date_bin=week')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/v0/lineages:count?group_by=collection_date&date_bin=day')
        self.assertTrue(res.ok)


if __name__ == '__main__':
    unittest.main()

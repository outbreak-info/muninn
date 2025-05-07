import unittest

import requests

from tests.integration import timing


class BasicApiTests(unittest.TestCase):
    """
    These tests assume that the server is running.
    I don't want to deal with the proper setup for this right now.

    This is as much a collection of urls as it is an actual test suite.
    """

    @timing
    def test_sample_id(self):
        res = requests.get('http://localhost:8000/sample/1')
        self.assertTrue(res.ok)

    @timing
    def test_phenotype_metrics(self):
        res = requests.get('http://localhost:8000/phenotype_metrics')
        self.assertTrue(res.ok)

    @timing
    def test_samples(self):
        res = requests.get('http://localhost:8000/samples?q=host=cattle')
        self.assertTrue(res.ok)

    @timing
    def test_variants(self):
        res = requests.get('http://localhost:8000/variants?q=ref_nt=A')
        self.assertTrue(res.ok)

    @timing
    def test_mutations(self):
        res = requests.get('http://localhost:8000/mutations?q=ref_aa=Q')
        self.assertTrue(res.ok)

    @timing
    def test_variants_by_sample(self):
        res = requests.get('http://localhost:8000/variants/by/sample?q=host=domestic cat')
        self.assertTrue(res.ok)

    @timing
    def test_mutations_by_sample(self):
        res = requests.get('http://localhost:8000/mutations/by/sample?q=country_name=usa')
        self.assertTrue(res.ok)

    @timing
    def test_samples_by_mutation(self):
        res = requests.get('http://localhost:8000/samples/by/mutation?q=ref_nt=A')
        self.assertTrue(res.ok)

    @timing
    def test_samples_by_variant(self):
        res = requests.get('http://localhost:8000/samples/by/variant?q=alt_freq>=0.9')
        self.assertTrue(res.ok)

    @timing
    def test_count_x_by_y(self):
        res = requests.get('http://localhost:8000/count/samples/by/host')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/count/variants/by/ref_dp')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/count/mutations/by/region')
        self.assertTrue(res.ok)

    @timing
    def test_variant_frequency(self):
        res = requests.get('http://localhost:8000/variants/frequency?aa=HA:A172T')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/variants/frequency?nt=HA:A172T')
        self.assertTrue(res.ok)

    @timing
    def test_mutation_count(self):
        res = requests.get('http://localhost:8000/mutations/frequency?aa=HA:A172T')
        self.assertTrue(res.ok)

        res = requests.get('http://localhost:8000/mutations/frequency?nt=HA:A172T')
        self.assertTrue(res.ok)

    @timing
    def test_variant_counts_by_phenotype_score(self):
        res = requests.get('http://localhost:8000/variants/frequency/score?region=HA&metric=stability')
        self.assertTrue(res.ok)

    @timing
    def test_mutation_counts_by_phenotype_score(self):
        res = requests.get('http://localhost:8000/mutations/frequency/score?region=HA&metric=stability')
        self.assertTrue(res.ok)

    @timing
    def test_sample_counts_by_lineage(self):
        res = requests.get('http://localhost:8000/count/samples/lineages?q=host=cattle')
        self.assertTrue(res.ok)

    @timing
    def test_lineage_abundance_info(self):
        res = requests.get('http://localhost:8000/lineages/abundances?q=host=chicken')
        self.assertTrue(res.ok)

    @timing
    def test_lineage_abundance_summary_stats(self):
        res = requests.get('http://localhost:8000/lineages/abundances/summary_stats?q=host=cattle')
        self.assertTrue(res.ok)


from datamart.metadata.global_metadata import GlobalMetadata
from datamart.metadata.variable_metadata import VariableMetadata
import unittest, copy, json
from termcolor import colored

gt = json.load(open('datamart/unit_tests/resources/trading_economic.json', "r"))
sample_global_metadata_description = gt["description"]


class TestGlobalMetadata(unittest.TestCase):
    def setUp(self):
        self.global_metadata_description = copy.deepcopy(sample_global_metadata_description)
        self.metadata = GlobalMetadata(description=self.global_metadata_description, datamart_id=0)

    def test_datamart_id(self):
        print("[Test]{}/test_datamart_id".format(self.__class__.__name__))
        self.assertEqual(self.metadata.datamart_id, 0)
        print(colored('.Done', 'red'))

    def test_title(self):
        print("[Test]{}/test_title".format(self.__class__.__name__))
        self.assertEqual(self.metadata.title, sample_global_metadata_description["title"])
        self.metadata.title = "fake_title"
        self.assertEqual(self.metadata.title, "fake_title")
        self.metadata.title = sample_global_metadata_description["title"]
        print(colored('.Done', 'red'))

    def test_description(self):
        print("[Test]{}/test_description".format(self.__class__.__name__))
        self.assertEqual(self.metadata.description, sample_global_metadata_description["description"])
        self.metadata.description = "fake_description"
        self.assertEqual(self.metadata.description, "fake_description")
        self.metadata.description = sample_global_metadata_description["description"]
        print(colored('.Done', 'red'))

    def test_url(self):
        print("[Test]{}/test_url".format(self.__class__.__name__))
        self.assertEqual(self.metadata.url, sample_global_metadata_description["url"])
        print(colored('.Done', 'red'))

    def test_keywords(self):
        print("[Test]{}/test_keywords".format(self.__class__.__name__))
        self.assertEqual(self.metadata.keywords, sample_global_metadata_description["keywords"])
        self.metadata.keywords = ["fake_keywords_1", "fake_keywords_3", "fake_keywords_2"]
        self.assertEqual(self.metadata.keywords, ["fake_keywords_1", "fake_keywords_3", "fake_keywords_2"])
        print(colored('.Done', 'red'))

    def test_date_published(self):
        print("[Test]{}/test_date_published".format(self.__class__.__name__))
        self.assertEqual(self.metadata.date_published, "2000-10-01T00:00:00")
        self.metadata.date_published = "2000-10-10"
        self.assertEqual(self.metadata.date_published, "2000-10-10")
        print(colored('.Done', 'red'))

    def test_date_updated(self):
        print("[Test]{}/test_date_updated".format(self.__class__.__name__))
        self.assertEqual(self.metadata.date_updated, None)
        self.metadata.date_updated = "2000-10-20"
        self.assertEqual(self.metadata.date_updated, "2000-10-20")
        print(colored('.Done', 'red'))

    def test_provenance(self):
        print("[Test]{}/test_provenance".format(self.__class__.__name__))
        self.assertEqual(self.metadata.provenance, sample_global_metadata_description.get("provenance", None))
        print(colored('.Done', 'red'))

    def test_original_identifier(self):
        print("[Test]{}/test_original_identifier".format(self.__class__.__name__))
        self.assertEqual(self.metadata.original_identifier,
                         sample_global_metadata_description.get("original_identifier", None))
        print(colored('.Done', 'red'))

    def test_materialization(self):
        print("[Test]{}/test_materialization".format(self.__class__.__name__))
        self.assertEqual(self.metadata.materialization["python_path"],
                         sample_global_metadata_description["materialization"]["python_path"])
        self.assertEqual(self.metadata.materialization["arguments"], None)
        print(colored('.Done', 'red'))

    def test_license(self):
        print("[Test]{}/test_license".format(self.__class__.__name__))
        self.assertEqual(self.metadata.license, sample_global_metadata_description.get("license", None))
        print(colored('.Done', 'red'))

    def test_add_variable(self):
        print("[Test]{}/test_add_variable".format(self.__class__.__name__))
        self.assertEqual(len(self.metadata.variables), 0)
        for col_offset, variable_description in enumerate(self.global_metadata_description["variables"]):
            variable_metadata = VariableMetadata(variable_description, datamart_id=col_offset+1)
            self.metadata.add_variable_metadata(variable_metadata)
        self.assertEqual(len(self.metadata.variables), len(sample_global_metadata_description["variables"]))
        self.assertEqual(self.metadata.value, gt["metadata"])
        print(colored('.Done', 'red'))

from datamart.metadata.variable_metadata import VariableMetadata
import unittest, copy
from termcolor import colored

sample_variable_1 = {
    "name": "DateTime",
    "description": "DateTime of the current value",
    "semantic_type": [
        "https://metadata.datadrivendiscovery.org/types/Time"
    ],
    "temporal_coverage": {
        "start": "1993-01-31T00:00:00",
        "end": "2018-08-31"
    }
}

sample_variable_2 = {
    "name": "FakeDate",
    "description": "DateTime of the current value",
    "semantic_type": [
        "https://metadata.datadrivendiscovery.org/types/Time",
        "https://metadata.datadrivendiscovery.org/types/CategoricalData"
    ],
    "temporal_coverage": {
        "start": "19sad",
        "end": "2018-08-31T00:00:00"
    },
    "spatial_coverage": "fake_spatial_coverage",
    "materialization_component": "fake_materialization_component"
}


class TestVariableMetadata(unittest.TestCase):
    def setUp(self):
        self.variable_1 = copy.deepcopy(sample_variable_1)
        self.variable_2 = copy.deepcopy(sample_variable_2)
        self.metadata_1 = VariableMetadata(description=self.variable_1, datamart_id=0)
        self.metadata_2 = VariableMetadata(description=self.variable_2, datamart_id=10)

    def test_datamart_id(self):
        print("[Test]{}/test_datamart_id".format(self.__class__.__name__))
        self.assertEqual(self.metadata_1.datamart_id, 0)
        self.assertEqual(self.metadata_2.datamart_id, 10)
        print(colored('.Done', 'red'))

    def test_name(self):
        print("[Test]{}/test_name".format(self.__class__.__name__))
        self.assertEqual(self.metadata_1.name, sample_variable_1["name"])
        self.assertEqual(self.metadata_2.name, sample_variable_2["name"])
        self.metadata_2.name = "fake_name"
        self.assertEqual(self.metadata_2.name, "fake_name")
        print(colored('.Done', 'red'))

    def test_description(self):
        print("[Test]{}/test_description".format(self.__class__.__name__))
        self.assertEqual(self.metadata_1.description, sample_variable_1["description"])
        self.assertEqual(self.metadata_2.description, sample_variable_2["description"])
        self.metadata_2.description = "fake_description"
        self.assertEqual(self.metadata_2.description, "fake_description")
        print(colored('.Done', 'red'))

    def test_semantic_type(self):
        print("[Test]{}/test_semantic_type".format(self.__class__.__name__))
        self.assertEqual(self.metadata_1.semantic_type, sample_variable_1["semantic_type"])
        self.assertEqual(self.metadata_2.semantic_type, sample_variable_2["semantic_type"])
        self.metadata_2.semantic_type = ["https://metadata.datadrivendiscovery.org/types/Time"]
        self.assertEqual(self.metadata_2.semantic_type, ["https://metadata.datadrivendiscovery.org/types/Time"])
        print(colored('.Done', 'red'))

    def test_named_entity(self):
        print("[Test]{}/test_named_entity".format(self.__class__.__name__))
        self.assertEqual(self.metadata_1.named_entity, False)
        self.assertEqual(self.metadata_2.named_entity, False)
        self.metadata_2.semantic_type = ["fake_entity_1", "fake_entity_3", "fake_entity_2"]
        self.assertEqual(self.metadata_2.semantic_type, ["fake_entity_1", "fake_entity_3", "fake_entity_2"])
        print(colored('.Done', 'red'))

    def test_temporal_coverage(self):
        print("[Test]{}/test_temporal_coverage".format(self.__class__.__name__))
        self.assertEqual(self.metadata_1.temporal_coverage, {
            "start": "1993-01-31T00:00:00",
            "end": "2018-08-31T00:00:00"
        })
        self.metadata_2.temporal_coverage = {
            "start": "1993-01-31T00:00:00",
            "end": "2018-08-31T00:00:00"
        }
        self.assertEqual(self.metadata_2.temporal_coverage, {
            "start": "1993-01-31T00:00:00",
            "end": "2018-08-31T00:00:00"
        })
        print(colored('.Done', 'red'))

    def test_spatial_coverage(self):
        print("[Test]{}/test_spatial_coverage".format(self.__class__.__name__))
        self.assertEqual(self.metadata_1.spatial_coverage, False)
        self.assertEqual(self.metadata_2.spatial_coverage, sample_variable_2["spatial_coverage"])
        print(colored('.Done', 'red'))

    def test_variable_metadata(self):
        print("[Test]{}/test_variable_metadata".format(self.__class__.__name__))
        expected = {"datamart_id": 0, "name": "DateTime", "description": "DateTime of the current value",
                    "semantic_type": ["https://metadata.datadrivendiscovery.org/types/Time"],
                    "temporal_coverage": {"start": "1993-01-31T00:00:00", "end": "2018-08-31T00:00:00"}}
        self.assertEqual(self.metadata_1.value, expected)
        print(colored('.Done', 'red'))

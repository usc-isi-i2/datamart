from datamart.metadata.variable_metadata import VariableMetadata
import unittest, copy

sample_variable_1 = {
      "name": "DateTime",
      "description": "DateTime of the current value",
      "semantic_type": [
        "https://metadata.datadrivendiscovery.org/types/Time"
      ],
      "named_entity": False,
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
      "named_entity": False,
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
        self.assertEqual(self.metadata_1.datamart_id, 0)
        self.assertEqual(self.metadata_2.datamart_id, 10)

    def test_name(self):
        self.assertEqual(self.metadata_1.name, sample_variable_1["name"])
        self.assertEqual(self.metadata_2.name, sample_variable_2["name"])

    def test_description(self):
        self.assertEqual(self.metadata_1.description, sample_variable_1["description"])
        self.assertEqual(self.metadata_2.description, sample_variable_2["description"])

    def test_semantic_type(self):
        self.assertEqual(self.metadata_1.semantic_type, sample_variable_1["semantic_type"])
        self.assertEqual(self.metadata_2.semantic_type, sample_variable_2["semantic_type"])
        self.metadata_2.semantic_type = ["https://metadata.datadrivendiscovery.org/types/Time"]
        self.assertEqual(self.metadata_2.semantic_type, ["https://metadata.datadrivendiscovery.org/types/Time"])

    def test_named_entity(self):
        self.assertEqual(self.metadata_1.named_entity, sample_variable_1["named_entity"])
        self.assertEqual(self.metadata_2.named_entity, sample_variable_2["named_entity"])
        self.metadata_2.semantic_type = ["fake_entity_1", "fake_entity_3", "fake_entity_2"]
        self.assertEqual(self.metadata_2.semantic_type, ["fake_entity_1", "fake_entity_3", "fake_entity_2"])

    def test_temporal_coverage(self):
        self.assertEqual(self.metadata_1.temporal_coverage, {
            "start": "1993-01-31T00:00:00",
            "end": "2018-08-31T00:00:00"
        })
        self.assertEqual(self.metadata_2.temporal_coverage, True)
        self.metadata_2.temporal_coverage = {
            "start": "1993-01-31T00:00:00",
            "end": "2018-08-31T00:00:00"
        }
        self.assertEqual(self.metadata_2.temporal_coverage, {
            "start": "1993-01-31T00:00:00",
            "end": "2018-08-31T00:00:00"
        })

    def test_spatial_coverage(self):
        self.assertEqual(self.metadata_1.spatial_coverage, None)
        self.assertEqual(self.metadata_2.spatial_coverage, sample_variable_2["spatial_coverage"])

    def test_materialization_component(self):
        self.assertEqual(self.metadata_1.materialization_component, None)
        self.assertEqual(self.metadata_2.materialization_component, sample_variable_2["materialization_component"])

    def test_variable_metadata(self):
        expected = {'datamart_id': 0, 'name': 'DateTime', 'description': 'DateTime of the current value',
                    'semantic_type': ['https://metadata.datadrivendiscovery.org/types/Time'], 'named_entity': False,
                    'temporal_coverage': {'start': '1993-01-31T00:00:00', 'end': '2018-08-31T00:00:00'},
                    'spatial_coverage': None, 'materialization_component': None}
        self.assertEqual(self.metadata_1.value, expected)

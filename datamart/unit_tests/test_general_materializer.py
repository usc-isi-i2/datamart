from datamart.materializers.general_materializer import GeneralMaterializer
import unittest
from datamart.utilities.utils import Utils


class TestGeneralMaterializer(unittest.TestCase):
    def setUp(self):
        self.general_materializer = GeneralMaterializer()

    @Utils.test_print
    def test_get_csv(self):
        mock_metadata = {
            "materialization": {
                "arguments": {
                    "url": "http://insight.dev.schoolwires.com/HelpAssets/C2Assets/C2Files/C2ImportFamRelSample.csv",
                    "file_type": "csv"
                }
            }
        }
        result = self.general_materializer.get(metadata=mock_metadata).to_csv(index=False)
        expected = "Parent Identifier,Student Identifier\n1001,1002\n1010,1020\n"
        self.assertEqual(result, expected)

    @Utils.test_print
    def test_get_html(self):
        mock_metadata = {
            "materialization": {
                "arguments": {
                    "url": "https://www.w3schools.com/html/html_tables.asp",
                    "file_type": "html"
                }
            }
        }
        result = self.general_materializer.get(metadata=mock_metadata).to_csv(index=False)
        expected = """Company,Contact,Country
Alfreds Futterkiste,Maria Anders,Germany
Centro comercial Moctezuma,Francisco Chang,Mexico
Ernst Handel,Roland Mendel,Austria
Island Trading,Helen Bennett,UK
Laughing Bacchus Winecellars,Yoshi Tannamuri,Canada
Magazzini Alimentari Riuniti,Giovanni Rovelli,Italy
"""
        self.assertEqual(result, expected)



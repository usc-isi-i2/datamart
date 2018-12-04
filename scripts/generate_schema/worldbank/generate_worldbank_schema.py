import os
from argparse import ArgumentParser
import requests
import json
import traceback

LOCATIONS = [
  "Aruba",
  "Afghanistan",
  "Africa",
  "Angola",
  "Albania",
  "Andorra",
  "Andean Region",
  "Arab World",
  "United Arab Emirates",
  "Argentina",
  "Armenia",
  "American Samoa",
  "Antigua and Barbuda",
  "Australia",
  "Austria",
  "Azerbaijan",
  "Burundi",
  "East Asia & Pacific (IBRD-only countries)",
  "Europe & Central Asia (IBRD-only countries)",
  "Belgium",
  "Benin",
  "Burkina Faso",
  "Bangladesh",
  "Bulgaria",
  "IBRD countries classified as high income",
  "Bahrain",
  "Bahamas, The",
  "Bosnia and Herzegovina",
  "Latin America & the Caribbean (IBRD-only countries)",
  "Belarus",
  "Belize",
  "Middle East & North Africa (IBRD-only countries)",
  "Bermuda",
  "Bolivia",
  "Brazil",
  "Barbados",
  "Brunei Darussalam",
  "Sub-Saharan Africa (IBRD-only countries)",
  "Bhutan",
  "Botswana",
  "Sub-Saharan Africa (IFC classification)",
  "Central African Republic",
  "Canada",
  "East Asia and the Pacific (IFC classification)",
  "Central Europe and the Baltics",
  "Europe and Central Asia (IFC classification)",
  "Switzerland",
  "Channel Islands",
  "Chile",
  "China",
  "Cote d'Ivoire",
  "Latin America and the Caribbean (IFC classification)",
  "Middle East and North Africa (IFC classification)",
  "Cameroon",
  "Congo, Dem. Rep.",
  "Congo, Rep.",
  "Colombia",
  "Comoros",
  "Cabo Verde",
  "Costa Rica",
  "South Asia (IFC classification)",
  "Caribbean small states",
  "Cuba",
  "Curacao",
  "Cayman Islands",
  "Cyprus",
  "Czech Republic",
  "East Asia & Pacific (IDA-eligible countries)",
  "Europe & Central Asia (IDA-eligible countries)",
  "Germany",
  "IDA countries classified as Fragile Situations",
  "Djibouti",
  "Latin America & the Caribbean (IDA-eligible countries)",
  "Dominica",
  "Middle East & North Africa (IDA-eligible countries)",
  "IDA countries not classified as Fragile Situations",
  "Denmark",
  "IDA countries in Sub-Saharan Africa not classified as fragile situations ",
  "Dominican Republic",
  "South Asia (IDA-eligible countries)",
  "IDA countries in Sub-Saharan Africa classified as fragile situations ",
  "Sub-Saharan Africa (IDA-eligible countries)",
  "IDA total, excluding Sub-Saharan Africa",
  "Algeria",
  "East Asia & Pacific (excluding high income)",
  "Early-demographic dividend",
  "East Asia & Pacific",
  "Europe & Central Asia (excluding high income)",
  "Europe & Central Asia",
  "Ecuador",
  "Egypt, Arab Rep.",
  "Euro area",
  "Eritrea",
  "Spain",
  "Estonia",
  "Ethiopia",
  "European Union",
  "Fragile and conflict affected situations",
  "Finland",
  "Fiji",
  "France",
  "Faroe Islands",
  "Micronesia, Fed. Sts.",
  "IDA countries classified as fragile situations, excluding Sub-Saharan Africa",
  "Gabon",
  "United Kingdom",
  "Georgia",
  "Ghana",
  "Gibraltar",
  "Guinea",
  "Gambia, The",
  "Guinea-Bissau",
  "Equatorial Guinea",
  "Greece",
  "Grenada",
  "Greenland",
  "Guatemala",
  "Guam",
  "Guyana",
  "High income",
  "Hong Kong SAR, China",
  "Honduras",
  "Heavily indebted poor countries (HIPC)",
  "Croatia",
  "Haiti",
  "Hungary",
  "IBRD, including blend",
  "IBRD only",
  "IDA & IBRD total",
  "IDA total",
  "IDA blend",
  "Indonesia",
  "IDA only",
  "Isle of Man",
  "India",
  "Not classified",
  "Ireland",
  "Iran, Islamic Rep.",
  "Iraq",
  "Iceland",
  "Israel",
  "Italy",
  "Jamaica",
  "Jordan",
  "Japan",
  "Kazakhstan",
  "Kenya",
  "Kyrgyz Republic",
  "Cambodia",
  "Kiribati",
  "St. Kitts and Nevis",
  "Korea, Rep.",
  "Kuwait",
  "Latin America & Caribbean (excluding high income)",
  "Lao PDR",
  "Lebanon",
  "Liberia",
  "Libya",
  "St. Lucia",
  "Latin America & Caribbean ",
  "Latin America and the Caribbean",
  "Least developed countries,ssification",
  "Low income",
  "Liechtenstein",
  "Sri Lanka",
  "Lower middle income",
  "Low & middle income",
  "Lesotho",
  "Late-demographic dividend",
  "Lithuania",
  "Luxembourg",
  "Latvia",
  "Macao SAR, China",
  "St. Martin (French part)",
  "Morocco",
  "Central America",
  "Monaco",
  "Moldova",
  "Middle East (developing only)",
  "Madagascar",
  "Maldives",
  "Middle East & North Africa",
  "Mexico",
  "Marshall Islands",
  "Middle income",
  "Macedonia, FYR",
  "Mali",
  "Malta",
  "Myanmar",
  "Middle East & North Africa (excluding high income)",
  "Montenegro",
  "Mongolia",
  "Northern Mariana Islands",
  "Mozambique",
  "Mauritania",
  "Mauritius",
  "Malawi",
  "Malaysia",
  "North America",
  "North Africa",
  "Namibia",
  "New Caledonia",
  "Niger",
  "Nigeria",
  "Nicaragua",
  "Netherlands",
  "Non-resource rich Sub-Saharan Africa countries, of which landlocked",
  "Norway",
  "Nepal",
  "Non-resource rich Sub-Saharan Africa countries",
  "Nauru",
  "IDA countries not classified as fragile situations, excluding Sub-Saharan Africa",
  "New Zealand",
  "OECD members",
  "Oman",
  "Other small states",
  "Pakistan",
  "Panama",
  "Peru",
  "Philippines",
  "Palau",
  "Papua New Guinea",
  "Poland",
  "Pre-demographic dividend",
  "Puerto Rico",
  "Korea, Dem. People’s Rep.",
  "Portugal",
  "Paraguay",
  "West Bank and Gaza",
  "Pacific island small states",
  "Post-demographic dividend",
  "French Polynesia",
  "Qatar",
  "Romania",
  "Resource rich Sub-Saharan Africa countries",
  "Resource rich Sub-Saharan Africa countries, of which oil exporters",
  "Russian Federation",
  "Rwanda",
  "South Asia",
  "Saudi Arabia",
  "Southern Cone",
  "Sudan",
  "Senegal",
  "Singapore",
  "Solomon Islands",
  "Sierra Leone",
  "El Salvador",
  "San Marino",
  "Somalia",
  "Serbia",
  "Sub-Saharan Africa (excluding high income)",
  "South Sudan",
  "Sub-Saharan Africa ",
  "Small states",
  "Sao Tome and Principe",
  "Suriname",
  "Slovak Republic",
  "Slovenia",
  "Sweden",
  "Eswatini",
  "Sint Maarten (Dutch part)",
  "Sub-Saharan Africa excluding South Africa",
  "Seychelles",
  "Syrian Arab Republic",
  "Turks and Caicos Islands",
  "Chad",
  "East Asia & Pacific (IDA & IBRD countries)",
  "Europe & Central Asia (IDA & IBRD countries)",
  "Togo",
  "Thailand",
  "Tajikistan",
  "Turkmenistan",
  "Latin America & the Caribbean (IDA & IBRD countries)",
  "Timor-Leste",
  "Middle East & North Africa (IDA & IBRD countries)",
  "Tonga",
  "South Asia (IDA & IBRD)",
  "Sub-Saharan Africa (IDA & IBRD countries)",
  "Trinidad and Tobago",
  "Tunisia",
  "Turkey",
  "Tuvalu",
  "Taiwan, China",
  "Tanzania",
  "Uganda",
  "Ukraine",
  "Upper middle income",
  "Uruguay",
  "United States",
  "Uzbekistan",
  "St. Vincent and the Grenadines",
  "Venezuela, RB",
  "British Virgin Islands",
  "Virgin Islands (U.S.)",
  "Vietnam",
  "Vanuatu",
  "World",
  "Samoa",
  "Kosovo",
  "Sub-Saharan Africa excluding South Africa and Nigeria",
  "Yemen, Rep.",
  "South Africa",
  "Zambia",
  "Zimbabwe"
]


def getAllIndicatorList():
    url = "https://api.worldbank.org/v2/indicators?format=json&page=1"
    res = requests.get(url)
    data = res.json()
    total = data[0]['total']
    url2 = "https://api.worldbank.org/v2/indicators?format=json&page=1&per_page=" + str(total)
    res2 = requests.get(url2)
    data2 = res2.json()
    return data2[1]


def generate_json_schema(dst_path):
    unique_urls_str = getAllIndicatorList()
    for commondata in unique_urls_str:
        try:
            urldata = "https://api.worldbank.org/v2/countries/indicators/" + commondata['id'] + "?format=json"
            resdata = requests.get(urldata)
            data_ind = resdata.json()
            print("Generating schema for Trading economics", commondata['name'])
            schema = {}
            schema["title"] = commondata['name']
            schema["description"] = commondata['sourceNote']
            schema["url"] = "https://api.worldbank.org/v2/indicators/" + commondata['id'] + "?format=json"
            schema["keywords"] = [i for i in commondata['name'].split()]
            schema["date_updated"] = data_ind[0]["lastupdated"] if data_ind else None
            schema["license"] = None
            schema["provenance"] = {"source": "http://worldbank.org"}
            schema["original_identifier"] = commondata['id']

            schema["materialization"] = {
                "python_path": "worldbank_materializer",
                "arguments": {
                    "url": "https://api.worldbank.org/v2/indicators/" + commondata['id'] + "?format=json"
                }
            }
            schema['variables'] = []
            first_col = {
                "name": "indicator_id",

                "description": "id is identifier of an indicator in worldbank datasets",
                "semantic_type": ["https://metadata.datadrivendiscovery.org/types/CategoricalData"]
            }
            second_col = {
                "name": "indicator_value",

                "description": "name of an indicator in worldbank datasets",
                "semantic_type": ["http://schema.org/Text"]
            }
            third_col = {
                "name": "unit",

                "description": "unit of value returned by this indicator for a particular country",
                "semantic_type": ["https://metadata.datadrivendiscovery.org/types/CategoricalData"]

            }
            fourth_col = {
                "name": "sourceNote",

                "description": "Long description of the indicator",
                "semantic_type": ["http://schema.org/Text"]
            }
            fifth_col = {
                "name": "sourceOrganization",
                "description": "Source organization from where Worldbank acquired this data",
                "semantic_type": ["http://schema.org/Text"]
            }
            sixth_col = {
                "name": "country_value",

                "description": "Country for which idicator value is returned",
                "semantic_type": ["https://metadata.datadrivendiscovery.org/types/Location"],
                "named_entity": LOCATIONS
            }
            seventh_col = {
                "name": "countryiso3code",

                "description": "Country iso code for which idicator value is returned",
                "semantic_type": ["https://metadata.datadrivendiscovery.org/types/Location"]
            }
            eighth_col = {
                "name": "date",

                "description": "date for which indictor value is returned for a particular country",
                "semantic_type": ["https://metadata.datadrivendiscovery.org/types/Time"],
                "temporal_coverage": {"start": "1950", "end": "2100"}

            }
            schema['variables'].append(first_col)
            schema['variables'].append(second_col)
            schema['variables'].append(third_col)
            schema['variables'].append(fourth_col)
            schema['variables'].append(fifth_col)
            schema['variables'].append(sixth_col)
            schema['variables'].append(seventh_col)
            schema['variables'].append(eighth_col)
            if dst_path:
                os.makedirs(dst_path + '/worldbank_schema', exist_ok=True)

                file = os.path.join(dst_path, 'worldbank_schema',
                                    "{}_description.json".format(commondata['id']))
            else:
                os.makedirs('WorldBank_schema', exist_ok=True)
                file = os.path.join('worldbank_schema',
                                    "{}_description.json".format(commondata['id']))

            with open(file, "w") as fp:
                json.dump(schema, fp, indent=2)
        except:
            traceback.print_exc()
            pass


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-o", "--dst", action="store", type=str, dest="dst_path")
    args, _ = parser.parse_known_args()
    generate_json_schema(args.dst_path)

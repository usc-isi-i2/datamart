import requests
import csv
import os
import json
import sys

CITYLIST = []
DEFAULT_HEADERS = {
    "token": 'QoCwZxSlvRuUHcKhflbujnBSOFhHvZoS'
}
DESCRIPTION_MAP = dict()
DESCRIPTION_MAP['PRCP'] = 'Precipitation (tenths of mm)'
DESCRIPTION_MAP['SNOW'] = 'Snowfall (mm)'
DESCRIPTION_MAP['SNWD'] = 'Snow depth (mm)'
DESCRIPTION_MAP['TMAX'] = 'Maximum temperature (tenths of degrees C)'
DESCRIPTION_MAP['TMIN'] = 'Minimum temperature (tenths of degrees C)'
DESCRIPTION_MAP['ACMC'] = 'Average cloudiness midnight to midnight from 30-second ceilometer data (percent)'
DESCRIPTION_MAP['ACMH'] = 'Average cloudiness midnight to midnight from manual observations (percent)'
DESCRIPTION_MAP['ACSC'] = 'Average cloudiness sunrise to sunset from 30-second ceilometer data (percent)'
DESCRIPTION_MAP['ACSH'] = 'Average cloudiness sunrise to sunset from manual observations (percent)'
DESCRIPTION_MAP['AWDR'] = 'Average daily wind direction (degrees)'
DESCRIPTION_MAP['AWND'] = 'Average daily wind speed (tenths of meters per second)'
DESCRIPTION_MAP['DAEV'] = 'Number of days included in the multiday evaporation total (MDEV)'
DESCRIPTION_MAP['DAPR'] = 'Number of days included in the multiday precipiation total (MDPR)'
DESCRIPTION_MAP['DASF'] = 'Number of days included in the multiday snowfall total (MDSF)'
DESCRIPTION_MAP['DATN'] = 'Number of days included in the multiday minimum temperature (MDTN)'
DESCRIPTION_MAP['DATX'] = 'Number of days included in the multiday maximum temperature (MDTX)'
DESCRIPTION_MAP['DAWM'] = 'Number of days included in the multiday wind movement (MDWM)'
DESCRIPTION_MAP['DWPR'] = 'Number of days with non-zero precipitation included in multiday precipitation total (MDPR)'
DESCRIPTION_MAP['EVAP'] = 'Evaporation of water from evaporation pan (tenths of mm)'
DESCRIPTION_MAP['FMTM'] = 'Time of fastest mile or fastest 1-minute wind (hours and minutes, i.e., HHMM)'
DESCRIPTION_MAP['FRGB'] = 'Base of frozen ground layer (cm)'
DESCRIPTION_MAP['FRGT'] = 'Top of frozen ground layer (cm)'
DESCRIPTION_MAP['FRTH'] = 'Thickness of frozen ground layer (cm)'
DESCRIPTION_MAP['GAHT'] = 'Difference between river and gauge height (cm)'
DESCRIPTION_MAP['MDEV'] = 'Multiday evaporation total (tenths of mm; use with DAEV)'
DESCRIPTION_MAP['MDPR'] = 'Multiday precipitation total (tenths of mm; use with DAPR and DWPR, if available)'
DESCRIPTION_MAP['MDSF'] = 'Multiday snowfall total'
DESCRIPTION_MAP['MDTN'] = 'Multiday minimum temperature (tenths of degrees C; use with DATN)'
DESCRIPTION_MAP['MDTX'] = 'Multiday maximum temperature (tenths of degress C; use with DATX)'
DESCRIPTION_MAP['MDWM'] = 'Multiday wind movement (km)'
DESCRIPTION_MAP['MNPN'] = 'Daily minimum temperature of water in an evaporation pan (tenths of degrees C)'
DESCRIPTION_MAP['MXPN'] = 'Daily maximum temperature of water in an evaporation pan (tenths of degrees C)'
DESCRIPTION_MAP['PGTM'] = 'Peak gust time (hours and minutes, i.e., HHMM)'
DESCRIPTION_MAP['PSUN'] = 'Daily percent of possible sunshine (percent)'
DESCRIPTION_MAP['TAVG'] = "Average temperature (tenths of degrees C)[Note that TAVG from source 'S' corresponds to an average for the period ending at 2400 UTC rather than local midnight]"
DESCRIPTION_MAP['THIC'] = 'Thickness of ice on water (tenths of mm)'
DESCRIPTION_MAP['TOBS'] = 'Temperature at the time of observation (tenths of degrees C)'
DESCRIPTION_MAP['TSUN'] = 'Daily total sunshine (minutes)'
DESCRIPTION_MAP['WDF1'] = 'Direction of fastest 1-minute wind (degrees)'
DESCRIPTION_MAP['WDF2'] = 'Direction of fastest 2-minute wind (degrees)'
DESCRIPTION_MAP['WDF5'] = 'Direction of fastest 5-second wind (degrees)'
DESCRIPTION_MAP['WDFG'] = 'Direction of peak wind gust (degrees)'
DESCRIPTION_MAP['WDFI'] = 'Direction of highest instantaneous wind (degrees)'
DESCRIPTION_MAP['WDFM'] = 'Fastest mile wind direction (degrees)'
DESCRIPTION_MAP['WDMV'] = '24-hour wind movement (km)'
DESCRIPTION_MAP['WESD'] = 'Water equivalent of snow on the ground (tenths of mm)'
DESCRIPTION_MAP['WESF'] = 'Water equivalent of snowfall (tenths of mm)'
DESCRIPTION_MAP['WSF1'] = 'Fastest 1-minute wind speed (tenths of meters per second)'
DESCRIPTION_MAP['WSF2'] = 'Fastest 2-minute wind speed (tenths of meters per second)'
DESCRIPTION_MAP['WSF5'] = 'Fastest 5-second wind speed (tenths of meters per second)'
DESCRIPTION_MAP['WSFG'] = 'Peak gust wind speed (tenths of meters per second)'
DESCRIPTION_MAP['WSFI'] = 'Highest instantaneous wind speed (tenths of meters per second)'
DESCRIPTION_MAP['WSFM'] = 'Fastest mile wind speed (tenths of meters per second)'


def fetch_city_table(next):
    URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/locations?locationcategoryid=CITY&limit=1000"
    method = 'w'
    if (next):
        URL = URL + "&offset=1001"
        method = 'a'

    response = requests.get(URL, headers=DEFAULT_HEADERS)
    data = response.json()
    new_dict = dict()
    for res in data['results']:
        new_dict[res['name'].split(',')[0].lower()] = res['id']
    path = os.path.join(os.path.dirname(__file__), '../../../datamart/resources/')
    with open(os.path.join(path, 'city_id_map.csv'), method) as csv_file:
        writer = csv.writer(csv_file)
        for key, value in new_dict.items():
            CITYLIST.append(key)
            writer.writerow([key, value])


def generate_json_schema():
    url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/datatypes?datasetid=GHCND&limit=1000&offset=1'
    response = requests.get(url, headers=DEFAULT_HEADERS)
    data = response.json()
    for result in data["results"]:
        schema = dict()
        schema['title'] = result['id']
        if result['id'] in DESCRIPTION_MAP:
            schema['description'] = DESCRIPTION_MAP[result['id']]
        else:
            schema['description'] = result['name']
        schema['url'] = 'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt'
        schema['keywords'] = [result['name']]
        schema['provenance'] = {'source': 'noaa.org'}
        schema['materialization'] = {
            "python_path": 'noaa_materializer',
            "arguments": {
                "type": result['id']
            }
        }

        schema['variables'] = []
        first_col = dict()
        first_col['name'] = 'date'
        first_col['description'] = 'the date of data'
        first_col['semantic_type'] = ["https://metadata.datadrivendiscovery.org/types/Time"]
        first_col['temporal_coverage'] = dict()
        first_col['temporal_coverage']['start'] = result['mindate']
        first_col['temporal_coverage']['end'] = result['maxdate']

        sec_col = dict()
        sec_col['name'] = 'stationId'
        sec_col['description'] = 'the id of station which has this data'
        sec_col['semantic_type'] = ["https://metadata.datadrivendiscovery.org/types/CategoricalData"]

        third_col = dict()
        third_col['name'] = 'city'
        third_col['description'] = 'the city data belongs to'
        third_col['semantic_type'] = ["https://metadata.datadrivendiscovery.org/types/Location"]
        third_col['named_entity'] = CITYLIST

        fourth_col = dict()
        fourth_col['name'] = result['id']
        if result['id'] in DESCRIPTION_MAP:
            fourth_col['description'] = DESCRIPTION_MAP[result['id']]
        else:
            fourth_col['description'] = result['name']
        fourth_col['semantic_type'] = ["http://schema.org/Float"]

        schema['variables'].append(first_col)
        schema['variables'].append(sec_col)
        schema['variables'].append(third_col)
        schema['variables'].append(fourth_col)

        description_path = sys.argv[1]
        os.makedirs(description_path, exist_ok=True)
        with open(os.path.join(description_path, "{}_description.json".format(result["id"])), "w") as fp:
            json.dump(schema, fp, indent=2)


if __name__ == '__main__':
    fetch_city_table(False)
    fetch_city_table(True)
    generate_json_schema()

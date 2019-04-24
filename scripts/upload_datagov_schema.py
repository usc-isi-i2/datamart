import argparse
import json

from pathlib import Path

from datamart.materializers.general_materializer import GeneralMaterializer
from datamart.index_builder import IndexBuilder


ES_INDEX = 'datamart_all'


def load_schema(ib: IndexBuilder, schema_file: Path) -> dict:
    result = {}
    with open(schema_file) as fp:
        schema = json.load(fp)
    if schema['materialization']['python_path'] == 'general_materializer':
        file_type = schema['materialization']['file_type'] if 'file_type' in schema['materialization'] else None
        url = schema['materialization']['arguments']['url']

        # Only upload what general materializer can handle
        if GeneralMaterializer.can_parse(url, file_type):
            print(f'Load : {file_type} {url}')
            result = ib.indexing(schema, ES_INDEX)
            print(result['datamart_id'], schema_file)
        else:
            # print(f'skip file type: {file_type} {url}')
            pass
    else:
        print(f"SKIP materializer: {schema['materialization']['python_path']}")
    return result


def load_directory(directory: Path):
    ib = IndexBuilder()
    success = 0
    for count, schema_file in enumerate(directory.glob('*.json')):
        result = load_schema(ib, schema_file)
        if result:
            success += 1
    print(f'Loaded {success} of {count}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Upload datagov dataset schemas from directory')
    parser.add_argument('schema_directory', help='Directory containing the schemas')
    args = parser.parse_args()
    load_directory(Path(args.schema_directory))
    # /nas/home/kyao/dsbox/datamart/datagov/new_datagov_metdata_3_27

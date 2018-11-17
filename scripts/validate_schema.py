import sys, os
sys.path.append(sys.path.append(os.path.join(os.path.dirname(__file__), '..')))
import argparse
from datamart.utilities.utils import Utils
import json


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Util functions')
    parser.add_argument('--validate_json', help='Validate json against schema. Provide a path to json file', default=None)

    args = parser.parse_args()
    if args.validate_json:
        description = json.load(open(args.validate_json, 'r'))
        try:
            Utils.validate_schema(description)
            print("Valid json")
        except:
            print("Invalid json")

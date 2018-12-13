from datamart.materializers.materializer_base import MaterializerBase

import os
import urllib.request
import sys
import csv
import copy
import json
from typing import List
from pprint import pprint
import re
import typing
from pandas import DataFrame
import traceback


class WikidataSPOMaterializer(MaterializerBase):
    property = ""

    def __init__(self, **kwargs):
        """ initialization and loading the city name to city id map

        """
        MaterializerBase.__init__(self, **kwargs)

    def get(self,
            metadata: dict = None,
            constrains: dict = None
            ) -> typing.Optional[DataFrame]:

        materialization_arguments = metadata["materialization"].get("arguments", {})
        self.property = materialization_arguments.get("property", "")

        materialization_arguments = metadata["materialization"].get("arguments", {})
        self.property = materialization_arguments.get("property", "")
        prefix = 'http://sitaware.isi.edu:8080/bigdata/namespace/wdq/sparql?query='
        format = '&format=json'
        result = dict()
        property_label = ""
        main_query_encoded = self._encode_url(self._formulate_main_query(self.property))
        try:
            print(prefix + main_query_encoded + format)
            main_query_req = urllib.request.Request(prefix + main_query_encoded + format)
            result, property_label = self._process_main_query(self._get_query_result(main_query_req))
        except Exception as err:
            print(err)
            traceback.print_tb(err.__traceback__)
            count = 0
            while(True):
                try:
                    main_query_encoded = self._encode_url(self._next(self._formulate_main_query(self.property), offset=count))
                    main_query_req = urllib.request.Request(prefix + main_query_encoded + format)
                    temp, property_label = self._process_main_query(self._get_query_result(main_query_req))
            # property_label = re.sub(r"\s+", '_', property_label)
                    count += 1
                    result.update(temp)
                except:
                    # print("property ", property, "count ", count)
                    break

        property_label = re.sub(r"\s+", '_', property_label)
        sep = ";"
        values = list(result.values())
        columns = ["source", "subject_label", "category", "prop_value", "value_label"]
        # for val in values:
            # col_name = col_name.union(set(val.keys()))
        # columns = list(col_name)
        rows = list()
        for k, v in result.items():
            v['value_label'] = list(filter(None, v['value_label']))
            v['value_label'] = list() if not any(v['value_label']) else list(v['value_label'])
            for k1, v1 in v.items():
                if k1 != "source":
                    # print(k1, v1)
                    v[k1] = sep.join(v1)
            rows.append(v)

        df = DataFrame(rows, columns=columns)
        # print(df)
        return df

    @staticmethod
    def _formulate_main_query(property):
        main_query = 'select distinct ?source ?source_l ?category ?prop_l ?prop_value ?know_as where{\
                        ?source wdt:' + property + ' ?prop_value.\
                        ?source rdfs:label ?source_l.\
                        ?source wdt:P31/rdfs:label ?category.\
                        filter (lang(?category)="en")\
                        filter (lang(?source_l)="en")\
                        wd:' + property + ' rdfs:label ?prop_l.\
                        filter (lang(?prop_l)="en")\
                        optional {?prop_value rdfs:label ?know_as.\
                                 filter (lang(?know_as)="en")}\
                        }'

        return main_query

    @staticmethod
    def _formulate_id_category_query(property):
        id_category_query = \
            'select distinct ?identifier ?l where{\
                ?source wdt:' + property + ' ?value.\
                    ?source ?id ?idValue.\
                    ?identifier ?ref ?id.\
                    optional {?value rdfs:label ?know_as.\
                    filter (lang(?know_as)="en")}\
                    ?identifier wikibase:directClaim ?id.\
                    ?identifier wikibase:propertyType wikibase:ExternalId.\
                    ?identifier rdfs:label ?l.\
                    ?identifier schema:description ?desc.\
                    filter (lang(?desc)="en")\
                    filter (lang(?l)="en")\
                    }\
                ORDER BY ?identifier'

        return id_category_query

    @staticmethod
    def _next(query_sent, offset):
        query_sent = query_sent + " LIMIT 1000 " + "OFFSET " + str(1000 * offset)
        return query_sent

    @staticmethod
    def _encode_url(url):
        encoded_url = urllib.parse.quote(url)
        return encoded_url

    @staticmethod
    def _get_query_result(query_req) -> List[dict]:
        data = {}
        with urllib.request.urlopen(query_req) as r:
            data = json.loads(r.read().decode('utf-8'))

        result = data['results']['bindings']
        return result

    @staticmethod
    def _process_id_category_query(data):
        ids = dict()
        for item in data:
            identifier = item['l']['value']
            ids[identifier] = set()

        return ids

    @staticmethod
    def _process_main_query(data):
        result = {}
        property_label = ""

        for item in data:
            category = item['category']['value'].strip()
            property_label = item['prop_l']['value'].strip()
            source = item['source']['value'].strip()
            prop_value = item['prop_value']['value'].strip()
            know_as = item['know_as']['value'].strip() if 'know_as' in item.keys() else None
            subject_l = item['source_l']['value'].strip()
            # id = item['id']['value'].strip()
            # id_l = item['id_l']['value'].strip()
            # id_value = item['id_value']['value'].strip()

            if source not in result.keys():
                result[source] = dict()
                result[source]['source'] = source
                result[source]['category'] = set()
                result[source]['prop_value'] = set()
                result[source]['subject_label'] = set()
                result[source]['value_label'] = set()
                # result[source].update(copy.deepcopy(ids))

            result[source]['prop_value'].add(prop_value)
            result[source]['category'].add(category)
            result[source]['subject_label'].add(subject_l)
            result[source]['value_label'].add(know_as)
            # result[source][id_l].add(id_value)

        # pprint("ss", result)
        return result, property_label

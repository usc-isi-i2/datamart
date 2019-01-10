from datamart.es_managers.query_manager import *
from pandas import DataFrame


class JSONQueryManager(QueryManager):
    @classmethod
    def match_key_value_pairs_by_query_mapping(cls, query2index: typing.List[tuple], query_object: dict) \
            -> typing.Optional[dict]:
        k_v_pairs = []
        for query_key, target_key in query2index:
            value = query_object.get(query_key)
            if not target_key or not value:
                continue
            k_v_pairs.append((target_key, value))
        if k_v_pairs:
            return cls.match_key_value_pairs(k_v_pairs, disjunctive_array_value=True)

    @classmethod
    def parse_json_query(cls, json_query: dict, df: DataFrame=None) -> typing.Optional[str]:
        # conjunction of dataset constrains and required_variables hit and desired_variable hit:
        outer_must = []

        dataset = json_query.get('dataset')
        if dataset:
            about = dataset.get('about')
            if about:
                # match any fields/values ...
                # TODO: preference on phrases matching
                outer_must.append(cls.match_any(about))
            keys_mapping = [
                ('name', 'title'),
                ('description', 'description'),
                ('keywords', 'keywords'),
                ('url', 'url')
            ]
            outer_must.append(cls.match_key_value_pairs_by_query_mapping(keys_mapping, dataset))

            for query_date_key, index_date_key in [
                ('date_published', 'date_published'),
                ('date_created', 'date_updated') # ???
            ]:
                if dataset.get(query_date_key):
                    start = dataset.get(query_date_key).get('after')
                    end = dataset.get(query_date_key).get('before')
                    date_range = cls.parse_date_range(start, end)
                    if date_range:
                        outer_must.append({
                            "range": {
                                index_date_key: date_range
                            }
                        })

        # deal with variable list:
        entity_parsers = {
            'temporal_entity': cls.parse_temporal_entity,
            'geospatial_entity': cls.parse_geospatial_entity,
            'generic_entity': cls.parse_generic_entity
        }

        for variables_key in ('required_variables', 'desired_variables'):
            nested_queries = []
            variables = json_query.get(variables_key, [])
            for variable in variables:
                nested_query = None
                _type = variable.get('type')
                parser = entity_parsers.get(_type)
                if parser:
                    nested_query = parser(variable)
                elif _type == 'dataframe_columns':
                    cols = [df.iloc[:, index] for index in variable.get('index', [])] \
                           or [df.loc[:, name] for name in variable.get('names', [])]
                    cols_query = [cls.match_some_terms_from_variables_array(col.unique().tolist()) for col in cols]
                    if cols_query:
                        nested_query = cls.conjunction_query(cols_query)
                if nested_query:
                    nested_queries.append(nested_query)
            if nested_queries:
                outer_must.append(cls.disjunction_query(nested_queries))

        if outer_must:
            full_query = {
                "query": cls.conjunction_query(outer_must)
            }
            # print(json.dumps(full_query, indent=2))
            return json.dumps(full_query)

    @classmethod
    def parse_temporal_entity(cls, entity: dict) -> dict:
        """

        Args:
            entity: {
                "type": "temporal_entity",
                "start": "2018-01-01",
                "end": "2018-01-02",
                "granularity": "day"
            }

        Returns: partial ES query corresponding to this entity

        """
        nested_body = cls.match_temporal_coverage(entity.get('start'), entity.get('end'))
        return nested_body
        # TODO: make use of 'granularity'

    @classmethod
    def parse_geospatial_entity(cls, entity: dict) -> dict:
        named_entities = entity.get('named_entities', {}).get('terms')
        if named_entities:
            nested_body = cls.match_some_terms_from_variables_array(named_entities)
            return nested_body
        # TODO: support 'circle' and 'bounding_box'

    @classmethod
    def parse_generic_entity(cls, entity: dict) -> dict:
        queries = []
        if 'about' in entity:
            queries.append({
                "nested": {
                    "path": "variables",
                    "query": cls.match_any(entity['about'])
                }
            })
        keys_mapping = [
            ('variable_name', 'variables.name'),
            ('variable_metadata', None),
            ('variable_description', None),
            ('variable_syntactic_type', None),
            ('variable_semantic_type', 'variables.semantic_type'),
            ('named_entities', 'variables.named_entity'),
            ('column_values', None)
        ]
        matches = cls.match_key_value_pairs_by_query_mapping(keys_mapping, entity)
        if matches:
            queries.append(matches)
        query_body = cls.conjunction_query(queries)
        return query_body

    @staticmethod
    def parse_date_range(start: str, end: str) -> typing.Optional[dict]:
        start = Utils.date_validate(date_text=start) if start else None
        end = Utils.date_validate(date_text=end) if end else None
        if not (start or end):
            return None
        range_query = {"format": "yyyy-MM-dd'T'HH:mm:ss"}
        if start:
            range_query["lte"] = start
        if end:
            range_query["gte"] = end
        return range_query

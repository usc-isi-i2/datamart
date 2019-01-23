from datamart.es_managers.query_manager import *
from pandas import DataFrame


class JSONQueryManager(QueryManager):

    DATAFRAME_COLUMNS = "dataframe_columns"
    TEMPORAL_ENTITY = "temporal_entity"
    GEOSPATIAL_ENTITY = "geospatial_entity"
    GENERIC_ENTITY = "generic_entity"

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
                match_about_outer = cls.match_any(about)
                match_about_inner = {
                    "nested": {
                        "path": "variables",
                        "query": match_about_outer
                    }
                }
                match_about = {"bool": {"should": [match_about_outer, match_about_inner]}}
                outer_must.append(match_about)
            keys_mapping = [
                ('name', 'title'),
                ('description', 'description'),
                ('keywords', 'keywords'),
                ('url', 'url')
            ]
            string_arrays = cls.match_key_value_pairs_by_query_mapping(keys_mapping, dataset, "match_phrase")
            if string_arrays:
                outer_must.append(string_arrays)

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
        for variables_key in ('required_variables', 'desired_variables'):
            nested_queries = []
            variables = json_query.get(variables_key, [])
            for idx, variable in enumerate(variables):
                nested_query = cls.parse_a_variable(variable, variables_key, idx, df)
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
    def parse_a_variable(cls, entity: dict, key: str, index: int, df: DataFrame=None) -> typing.Optional[dict]:
        """
        for a entity in "required_variables" or "desired_variables", parse it to a nested ES query object.

        Args:
            entity: dict - the object for an entity
            key: str - either "required_variables" or "desired_variables"
            index: int - index in the "required_variables" or "desired_variables" array
            df: pandas.DataFrame - the original data

        Returns:
            the parsed ES query object

        """
        entity_type = entity.get('type')
        inner_match_name = '%s.%d.%s' % (key, index, entity_type)
        nested_query = None
        if entity_type == cls.DATAFRAME_COLUMNS:
            nested_query = cls.parse_dataframe_columns(entity, df)
        elif entity_type == cls.TEMPORAL_ENTITY:
            nested_query = cls.parse_temporal_entity(entity)
        elif entity_type == cls.GEOSPATIAL_ENTITY:
            nested_query = cls.parse_geospatial_entity(entity)
        elif entity_type == cls.GENERIC_ENTITY:
            nested_query = cls.parse_generic_entity(entity)

        if nested_query:
            cls.add_inner_hits_name(nested_query, inner_match_name)
            return nested_query

    @classmethod
    def parse_dataframe_columns(cls, entity: dict, df: DataFrame) -> typing.Optional[dict]:
        col_type = None
        if entity.get('index'):
            col_type = 'index'
        elif entity.get('names'):
            col_type = 'names'
        if col_type:
            cols_query = []
            for col in entity.get(col_type):
                terms = df.loc[:, col].unique().tolist() if col_type == 'names' \
                    else df.iloc[:, col].unique().tolist()
                col_query = cls.match_some_terms_from_variables_array(terms)
                cols_query.append(col_query)
            if cols_query:
                return cls.conjunction_query(cols_query)

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
        named_entities = entity.get('named_entities', {}).get('items')
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
                    "query": cls.match_any(entity['about']),
                    "inner_hits": {}
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
        # if the value is array of strings, should be "match_phrase", else "match" ?
        matches = cls.match_key_value_pairs_by_query_mapping(keys_mapping, entity, "match_phrase")
        if matches:
            queries.append(matches)
        if queries:
            return cls.conjunction_query(queries)

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

    @classmethod
    def match_key_value_pairs_by_query_mapping(cls, query2index: typing.List[tuple], query_object: dict,
                                               match_method: str="match") -> typing.Optional[dict]:
        k_v_pairs = []
        for query_key, target_key in query2index:
            value = query_object.get(query_key)
            if not target_key or not value:
                continue
            k_v_pairs.append((target_key, value))
        if k_v_pairs:
            return cls.match_key_value_pairs(k_v_pairs, disjunctive_array_value=True, match_method=match_method)

    @classmethod
    def add_inner_hits_name(cls, root, name):
        if root.get("nested"):
            try:
                root["nested"]["inner_hits"]["name"] = name
            except:
                pass
        else:
            try:
                for inner in root["bool"]["must"]:
                    cls.add_inner_hits_name(inner, name)
            except:
                pass

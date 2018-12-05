from datamart.metadata.metadata_base import MetadataBase
import dateutil.parser


class MetadataCluster(MetadataBase):

    def __init__(self, metadata_cluster):
        super().__init__()
        self._metadata = metadata_cluster

    @classmethod
    def construct_global(cls, docs) -> 'MetadataCluster':
        coverage_start = None
        coverage_end = None
        named_entities = set()
        datamart_id_lst = list()
        for doc in docs:
            datamart_id_lst.append(doc["datamart_id"])
            if "temporal_coverage" in doc:
                if doc.get("temporal_coverage", {}).get("start"):
                    if not coverage_start:
                        coverage_start = dateutil.parser.parse(doc["temporal_coverage"]["start"])
                    else:
                        coverage_start = min(coverage_start, dateutil.parser.parse(doc["temporal_coverage"]["start"]))
                if doc.get("temporal_coverage", {}).get("end"):
                    if not coverage_end:
                        coverage_end = dateutil.parser.parse(doc["temporal_coverage"]["end"])
                    else:
                        coverage_end = max(coverage_end, dateutil.parser.parse(doc["temporal_coverage"]["end"]))

            for implicit_variable in doc.get("implicit_variables", []):
                if "https://metadata.datadrivendiscovery.org/types/Time" not in implicit_variable["semantic_type"]:
                    named_entities.add(implicit_variable["value"])

            for variable in doc.get("variables", []):
                if "named_entity" in variable:
                    named_entities = named_entities.union(variable["named_entity"])

        return cls(metadata_cluster={
            "temporal_coverage": {
                "start": coverage_start,
                "end": coverage_end
            },
            "named_entity": list(named_entities),
            "datamart_ids": datamart_id_lst
        })

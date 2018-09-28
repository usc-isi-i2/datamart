from datamart.metadata.metadata_base import MetadataBase
import typing


class VariableMetadata(MetadataBase):
    def __init__(self, description: typing.Dict, datamart_id: int):
        """Init method of VariableMetadata.

        Args:
            description: description dict.
            datamart_id: unique datamart_id.

        Returns:

        """

        super().__init__()

        self._metadata["datamart_id"] = datamart_id

        try:
            self._metadata["title"] = description["title"]
        except:
            raise ValueError("No title found")

        try:
            self._metadata["description"] = description["description"]
        except:
            raise ValueError("No description found")

        self._metadata["semantic_type"] = description.get("semantic_type", [])
        self._metadata["named_entity"] = description.get("named_entity", None)
        self._metadata["temporal_coverage"] = description.get("temporal_coverage", [])
        self._metadata["spatial_coverage"] = description.get("named_entity", None)
        self._metadata["materialization_component"] = description.get("materialization_component", None)

    @property
    def datamart_id(self):
        return self._metadata["datamart_id"]

    @property
    def title(self):
        return self._metadata["title"]

    @property
    def description(self):
        return self._metadata["description"]

    @property
    def semantic_type(self):
        return self._metadata["semantic_type"]

    @semantic_type.setter
    def semantic_type(self, value):
        self._metadata["semantic_type"] = value

    @property
    def named_entity(self):
        return self._metadata["named_entity"]

    @named_entity.setter
    def named_entity(self, value):
        self._metadata["named_entity"] = value

    @property
    def temporal_coverage(self):
        return self._metadata["temporal_coverage"]

    @temporal_coverage.setter
    def temporal_coverage(self, value):
        self._metadata["temporal_coverage"] = value

    @property
    def spatial_coverage(self):
        return self._metadata["spatial_coverage"]

    @spatial_coverage.setter
    def spatial_coverage(self, value):
        self._metadata["spatial_coverage"] = value

    @property
    def materialization_component(self):
        return self._metadata["materialization_component"]

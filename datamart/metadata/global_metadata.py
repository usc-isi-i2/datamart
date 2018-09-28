from datamart.metadata.metadata_base import MetadataBase
from datamart.metadata.variable_metadata import VariableMetadata
import typing


class GlobalMetadata(MetadataBase):
    def __init__(self, description: typing.Dict, datamart_id: int):
        """Init method of GlobalMetadata.

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

        self._metadata["url"] = description.get("url", None)
        self._metadata["keywords"] = description.get("keywords", None)
        self._metadata["date_published"] = description.get("date_published", None)
        self._metadata["date_updated"] = description.get("date_updated", None)
        self._metadata["provenance"] = description.get("provenance", None)
        self._metadata["original_identifier"] = description.get("original_identifier", None)

        try:
            self._metadata["materialization"] = description["materialization"]
        except:
            raise ValueError("No materialization method found")

        self._metadata["materialization_component"] = description.get("materialization_component", None)
        self._metadata["variables"] = list()
        self._variables = list()

    def add_variable_metadata(self, variable_metadata: VariableMetadata):
        """Add a variable_metadata to this golbal metadata instance.

        Args:
            variable_metadata: VariableMetadata instance need to be added.

        Returns:

        """

        self._variables.append(variable_metadata)
        self._metadata["variables"].append(variable_metadata.value)

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
    def url(self):
        return self._metadata["url"]

    @property
    def keywords(self):
        return self._metadata["keywords"]

    @property
    def date_published(self):
        return self._metadata["date_published"]

    @property
    def date_updated(self):
        return self._metadata["date_updated"]

    @property
    def provenance(self):
        return self._metadata["provenance"]

    @property
    def original_identifier(self):
        return self._metadata["original_identifier"]

    @property
    def materialization(self):
        return self._metadata["materialization"]

    @property
    def materialization_component(self):
        return self._metadata["materialization_component"]

    @property
    def variables(self):
        return self._variables

    @property
    def variable_values(self):
        return self._metadata["variables"]

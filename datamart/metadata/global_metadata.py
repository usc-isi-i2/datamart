from datamart.metadata.metadata_base import MetadataBase
from datamart.metadata.variable_metadata import VariableMetadata
from datamart.utils import Utils


class GlobalMetadata(MetadataBase):
    def __init__(self, description: dict, datamart_id: int):
        """Init method of GlobalMetadata.

        Args:
            description: description dict.
            datamart_id: unique datamart_id.

        Returns:

        """

        super().__init__()

        self._metadata["datamart_id"] = datamart_id
        self._metadata["title"] = description["title"]

        self._metadata["description"] = description["description"]

        self._metadata["url"] = description.get("url", None)
        self._metadata["keywords"] = description.get("keywords", None)

        self._metadata["date_published"] = description.get("date_published", None)
        if self.date_published:
            self.date_published = Utils.date_validate(self.date_published)

        self._metadata["date_updated"] = description.get("date_updated", None)
        if self.date_updated:
            self.date_updated = Utils.date_validate(self.date_updated)

        self._metadata["provenance"] = description.get("provenance", None)
        self._metadata["original_identifier"] = description.get("original_identifier", None)

        try:
            self._metadata["materialization"] = description["materialization"]
        except:
            raise ValueError("No materialization found")

        if "python_path" not in self.materialization:
            raise ValueError("No python path found in materialization")

        if "arguments" not in self.materialization:
            self._metadata["materialization"]["arguments"] = None

        self._metadata["variables"] = list()
        self._variables = list()

        self._metadata["license"] = description.get("license", None)

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

    @title.setter
    def title(self, value):
        self._metadata["title"] = value

    @property
    def description(self):
        return self._metadata["description"]

    @description.setter
    def description(self, value):
        self._metadata["description"] = value

    @property
    def url(self):
        return self._metadata["url"]

    @property
    def keywords(self):
        return self._metadata["keywords"]

    @keywords.setter
    def keywords(self, value):
        self._metadata["keywords"] = value

    @property
    def date_published(self):
        return self._metadata["date_published"]

    @date_published.setter
    def date_published(self, value):
        self._metadata["date_published"] = value

    @property
    def date_updated(self):
        return self._metadata["date_updated"]

    @date_updated.setter
    def date_updated(self, value):
        self._metadata["date_updated"] = value

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
    def variables(self):
        return self._variables

    @property
    def variable_values(self):
        return self._metadata["variables"]

    @property
    def license(self):
        return self._metadata["license"]

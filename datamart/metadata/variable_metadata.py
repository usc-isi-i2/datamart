from datamart.metadata.metadata_base import MetadataBase


class VariableMetadata(MetadataBase):
    def __init__(self):
        super().__init__()
        self._metadata = {}

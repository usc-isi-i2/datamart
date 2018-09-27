from datamart.metadata.metadata_base import MetadataBase


class GlobalMetadata(MetadataBase):
    def __init__(self):
        super().__init__()
        self.metadata = {}

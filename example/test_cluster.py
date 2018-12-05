import sys, os, json

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from datamart.metadata_cluster_builder import MetadataClusterBuilder
from datamart.augment import Augment
import pandas as pd

# cluster_es_index="datamart_cluster"
#
# metadata_cluster_builder = MetadataClusterBuilder(metadata_es_index="datamart_fbi", cluster_es_index=cluster_es_index)
#
# metadata_cluster = metadata_cluster_builder.get_metadata_cluster_by_source(source="ucr.fbi.gov")
# metadata_cluster_builder.index_metadata_cluster(metadata_cluster=metadata_cluster, delete_old_es_index=True)

augment = Augment(es_index="datamart_all")

df = pd.DataFrame(data={"state": ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
                                  "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois",
                                  "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
                                  "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana",
                                  "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York",
                                  "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania",
                                  "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah",
                                  "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"]})


augment.query(col=df["state"],
              temporal_coverage_start="2010-01-01T00:00:00",
              temporal_coverage_end="2014-12-31T00:00:00")

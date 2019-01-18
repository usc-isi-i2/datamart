from datamart.utilities.utils import Utils
from datamart import search, augment


schema = {
    "materialization": {
        "python_path": "general_materializer",
        "arguments": {
            "url": "https://en.wikipedia.org/wiki/List_of_Rock_and_Roll_Hall_of_Fame_inductees",
            "file_type": "html"
        }
    }
}
hof_df = Utils.get_dataset(schema)

print(hof_df)

query = {
    "dataset": {
        "about": "rock and roll, music, rock music, rock artist, rock band, music award, artist award, hall of fame, singer"
    },
    "required_variables": [
        {
            "type": "dataframe_columns",
            "index": [2]
        }
    ]
}
candidates = search(query, hof_df)

res = []

for cand in candidates:
    res.append(augment(hof_df, cand))


for r in res:
    print('---')
    print(r)

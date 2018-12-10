import sys, os
import pandas as pd
import json

sys.path.append(os.path.join(os.getcwd(), '../..'))
from datamart.joiners.rltk_joiner import RLTKJoiner


def run_from_files(left_fp, right_fp, left_meta_fp, right_meta_fp, left_cols, right_cols):
    left_df = pd.read_csv(left_fp)
    right_df = pd.read_csv(right_fp)

    with open(left_meta_fp) as f_left:
        left_metadata = json.load(f_left)

    with open(right_meta_fp) as f_right:
        right_metadata = json.load(f_right)

    joiner = RLTKJoiner()

    res = joiner.join(
        left_df=left_df,
        right_df=right_df,
        left_columns=left_cols,
        right_columns=right_cols,
        left_metadata=left_metadata,
        right_metadata=right_metadata,
    )

    return res


# taxi example:
res = run_from_files('taxi/left.csv', 'taxi/right.csv', 'taxi/left.json', 'taxi/right.json', [[1], [3]], [[0], [2]])
print(res[['tpep_pickup_datetime', 'num_pickups', 'city', 'AWND']])


# fifa example: [[3], [4]] [[22], [24]]
res = run_from_files('fifa/left.csv', 'fifa/right.csv', 'fifa/left.json', 'fifa/right.json', [[3, 4]], [[22, 24]])
# print(res)
print(res[['Team', 'Opponent', 'homeTeam_id', 'awayTeam_id']])
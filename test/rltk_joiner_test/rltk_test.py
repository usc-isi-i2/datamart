import sys, os
import pandas as pd
import json

sys.path.append(os.path.join(os.getcwd(), '../..'))
from datamart.joiners.rltk_joiner import RLTKJoiner


left_df = pd.read_csv('left_df.csv')
right_df = pd.read_csv('right_df.csv')

with open('left_metadata.json') as f_left:
    left_metadata = json.load(f_left)
    # int64 was transformed to string !!!

with open('right_metadata.json') as f_left:
    right_metadata = json.load(f_left)
    # int64 was transformed to string !!!


joiner = RLTKJoiner()

res = joiner.join(
    left_df=left_df,
    right_df=right_df,
    left_columns=[[1], [3]],
    right_columns=[[0], [2]],
    left_metadata=left_metadata,
    right_metadata=right_metadata,
)

print(res)
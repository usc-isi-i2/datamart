from datamart import search, join
from datamart.utilities.utils import SEARCH_URL
import pandas as pd
from io import StringIO


# left = pd.read_csv(college.csv)
# right = pd.read_csv('right.csv')
#
# res = join(left, right, [[2]], [[3]])
#
# print(res)



# res = search(SEARCH_URL, {'required_variables': [{'type': 'dataframe_columns','names': ['INSTNM']}]}, 'college.csv')
#
# print(res[2].summary)
#
# print(res[2].materialize())
#
#
# joined = join('college.csv', res[2], [[2]], [[3]])
#
# print(joined)

from sklearn import metrics
from sklearn import linear_model
import numpy as np
from time import time


exclude = {"STABBR", "INSTNM", "UNITID", "DEBT_EARNINGS_RATIO", "CITY", "INSTURL", "STABBR.1", "NPCURL"}
non_var = "PrivacySuppressed"


# def fill_median(dataset):
#     col_median = np.nanmedian(dataset, axis=0)
#     inds = np.where(np.isnan(dataset))
#     dataset[inds] = np.take(col_median, inds[1])


def load_college(file_path, calc_cols, flag_col=None, test=False):
    full = pd.read_csv(file_path, na_values=[non_var])
    dataset = full.iloc[:, calc_cols]
    dataset = dataset.fillna(dataset.median()).values

    if test or (not flag_col):
        return dataset, None

    return dataset, full.iloc[:, flag_col].values


def eval(training, labels, testing, targets):
    reg = linear_model.Ridge(alpha=.1)
    reg.fit(training, labels)
    results = reg.predict(testing)

    score = metrics.explained_variance_score(targets, results)
    print("score: ", score)
    return score


print("LOAD ORIGINAL DTASET", time())

calc_cols = [3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
flag_col = 18
train_ = './college_datasets/college_train.csv'
test_ = './college_datasets/college_test.csv'
train_dataset, flags = load_college(train_, calc_cols, flag_col, False)
test_dataset, _ = load_college(test_, calc_cols, flag_col, True)
ground_truth_file = './college_datasets/college_targets.csv'
ground_truth = pd.read_csv(ground_truth_file).iloc[:, 1].values


print("TRAINING AND TESTING ORIGINAL DATASET", time())

score1 = eval(train_dataset, flags, test_dataset, ground_truth)


# -----

print("QUERY DATAMART", time())
ori = pd.read_csv(train_)
query = {
    "dataset": {
        "about": "college, university, education, earning"
    },
    "required_variables": [
        {"type": "dataframe_columns", "index": [2]}
    ]
}
res = search(SEARCH_URL, query, ori, return_named_entity=False)
# 291330000 291340000 291320000 291350000
for r in res:
    print(r.summary)


def augment_college(file_path, aug_dataset):
    cache_path = 'augmented_' + file_path.rsplit('/', 1)[-1]
    try:
        augmented = pd.read_csv(cache_path)
    except:
        print("JOIN WITH A DATASET", time())
        augmented = join(pd.read_csv(file_path), aug_dataset, [[2]], [[3]])
        with open('augmented_' + file_path.rsplit('/', 1)[-1], 'w') as fout:
            fout.write(augmented.to_csv(index=False))

    print(augmented.head())

    count_list = []
    for idx, name in enumerate(augmented.columns):
        if name not in exclude:
            count_list.append(idx)

    return load_college(StringIO(augmented.to_csv(index=False)), count_list)


print("PROCESS AUGMENTED DATASET AS NEW TRAINING DATA", time())
train_np_aug, _ = augment_college(train_, res[2])
test_np_aug, _ = augment_college(test_, res[2])


print("TRAINING AND TESTING AUGMENTED DATASET", time())
score2 = eval(train_np_aug, flags, test_np_aug, ground_truth)

print("--- END ---", time())
print("Original score: ", score1)
print("Augmented score: ", score2)










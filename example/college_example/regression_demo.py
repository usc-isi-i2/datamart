from datamart import join
import pandas as pd
from io import StringIO
import os

from sklearn import metrics
from sklearn import linear_model

exclude = {"STABBR", "INSTNM", "UNITID", "DEBT_EARNINGS_RATIO", "CITY", "INSTURL", "STABBR.1", "NPCURL"}
non_var = "PrivacySuppressed"


def load_college(file_path, calc_cols, flag_col=None, test=False):
    full = pd.read_csv(file_path, na_values=[non_var])
    dataset = full.iloc[:, calc_cols]
    dataset = dataset.fillna(dataset.median()).values

    if test or (not flag_col):
        return full, dataset, None

    return full, dataset, full.iloc[:, flag_col].values


def eval_regression(training, labels, testing, targets):
    reg = linear_model.Ridge(alpha=.1)
    reg.fit(training, labels)
    results = reg.predict(testing)

    scores = {}
    for k, v in [("explained_variance_score", metrics.explained_variance_score),
                 ("r2_score", metrics.r2_score),
                 ("mean_squared_error", metrics.mean_squared_error),
                 ("mean_absolute_error", metrics.mean_absolute_error),
                 ("mean_squared_log_error", metrics.mean_squared_log_error)]:
        try:
            s = v(targets, results)
            scores[k] = s
        except Exception as e:
            print("Failed on %s: %s" % (k, str(e)))
    return scores


def augment_college(file_path, aug_dataset):
    cache_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'augmented_' + file_path.rsplit('/', 1)[-1])
    print(cache_path)
    try:
        augmented = pd.read_csv(cache_path)
    except:
        augmented = join(pd.read_csv(file_path), aug_dataset, [[2]], [[3]])
        with open('augmented_' + file_path.rsplit('/', 1)[-1], 'w') as fout:
            fout.write(augmented.to_csv(index=False))

    print(augmented.head())

    count_list = []
    for idx, name in enumerate(augmented.columns):
        if name not in exclude:
            count_list.append(idx)

    return load_college(StringIO(augmented.to_csv(index=False)), count_list)


calc_cols = [3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
flag_col = 18











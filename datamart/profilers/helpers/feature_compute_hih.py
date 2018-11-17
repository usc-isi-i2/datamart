import pandas as pd  # type: ignore
import numpy as np  # type: ignore
from collections import Counter
from builtins import filter


def ordered_dict2(column, k):
    unique, counts = np.unique(column, return_counts=True)
    d = dict(zip(unique, counts))
    dlist = []
    for k, v in Counter(d).most_common(k):
        e = {'name': k, 'count': v}
        dlist.append(e)
    return dlist


def ordered_dict(column, k):
    # d = column.value_counts()[:k].to_dict()
    d = column.value_counts().head(k).to_dict()
    dlist = []
    for k, v in sorted(d.items(), key=lambda x: x[1], reverse=True):
        e = {'name': k, 'count': v}
        dlist.append(e)
    return dlist


def tryConvert(cell):
    """
    convert a cell, if possible, to its supposed type(int, float, string)
    note: type of NaN cell is float
    """
    try:
        return int(cell)
    except ValueError as TypeError:
        try:
            return float(cell)
        except ValueError as TypeError:
            return cell


def is_Decimal_Number(s):
    try:
        float(s)
        return True
    except:
        return False


def numerical_stats(feature, column, num_nonblank, feature_list):
    """
    calculates numerical statistics
    """
    ## hyper-parameter ##
    sigma = 3
    ## =============== ##
    stats = column.describe()
    if ("number_of_numeric_values" in feature_list):
        feature["number_of_numeric_values"] = int(stats["count"])
    if ("ratio_of_numeric_values" in feature_list):
        feature["ratio_of_numeric_values"] = stats["count"] / num_nonblank
    # feature["number_mean"] = stats["mean"]
    # feature["number_std"] = stats["std"]
    if stats["count"] == 1: feature["number_std"] = 0
    # feature["number_quartile_1"] = stats["25%"]
    # feature["number_quartile_2"] = stats["50%"]
    # feature["number_quartile_3"] = stats["75%"]
    if ("number_of_outlier_numeric_values" in feature_list):
        outlier = column[(np.abs(column - stats["mean"]) > (sigma * stats["std"]))]
        feature["number_of_outlier_numeric_values"] = outlier.count()
    if ("number_of_positive_numeric_values" in feature_list):
        feature["number_of_positive_numeric_values"] = column[column > 0].count()
    if ("number_of_negative_numeric_values" in feature_list):
        feature["number_of_negative_numeric_values"] = column[column < 0].count()
    if ("number_of_numeric_values_equal_0" in feature_list):
        feature["number_of_numeric_values_equal_0"] = column[column == 0].count()
    if ("number_of_numeric_values_equal_1" in feature_list):
        feature["number_of_numeric_values_equal_1"] = column[column == 1].count()
    if ("number_of_numeric_values_equal_-1" in feature_list):
        feature["number_of_numeric_values_equal_-1"] = column[column == -1].count()

    if ("target_values" in feature_list):
        feature["target_values"] = {'mean': stats['mean'],
                                    'std': stats['std'],
                                    'median': stats['50%'],
                                    'quartile_1': stats['25%'],
                                    'quartile_3': stats['75%']}


def compute_numerics(column, feature, feature_list):
    """
    computes numerical features of the column:
    # of integers/ decimal(float only)/ nonblank values in the column
    statistics of int/decimal/numerics
    """

    # if one of them is specified, then go next
    if (("number_of_numeric_values" in feature_list) or
            ("ratio_of_numeric_values" in feature_list) or
            ("number_std" in feature_list) or
            ("number_of_outlier_numeric_values" in feature_list) or
            ("number_of_negative_numeric_values" in feature_list) or
            ("number_of_positive_numeric_values" in feature_list) or
            ("number_of_numeric_values_equal_0" in feature_list) or
            ("number_of_numeric_values_equal_1" in feature_list) or
            ("number_of_numeric_values_equal_-1" in feature_list) or
            ("target_values" in feature_list)):

        cnt = column.count()
        if column.dtype.kind in np.typecodes['AllInteger'] + 'uf' and cnt > 0:
            numerical_stats(feature, column, cnt, feature_list)
        else:
            convert = lambda v: tryConvert(v)
            col = column.apply(convert, convert_dtype=False)

            col_nonblank = col.dropna()
            col_num = pd.Series([e for e in col_nonblank if
                                 type(e) == int or type(e) == np.int64 or type(e) == float or type(e) == np.float64])

            if col_num.count() > 0:
                numerical_stats(feature, col_num, cnt, feature_list)


def compute_common_numeric_tokens(column, feature, k):
    """
    compute top k frequent numerical tokens and their counts.
    tokens are integer or floats
    e.g. "123", "12.3"
    """
    col = np.asarray([token for lst in column.str.split().dropna() for token in lst])
    token = np.array(list(filter(lambda x: is_Decimal_Number(x), col)))
    if token.size:
        feature["most_common_numeric_tokens"] = ordered_dict2(token, k)


def compute_common_alphanumeric_tokens(column, feature, k):
    """
    compute top k frequent alphanumerical tokens and their counts.
    tokens only contain alphabets and/or numbers, decimals with points not included
    """
    col = np.asarray([token for lst in column.str.split().dropna() for token in lst])
    token = np.array(list(filter(lambda x: x.isalnum(), col)))
    if token.size:
        feature["most_common_alphanumeric_tokens"] = ordered_dict2(token, k)


def compute_common_values(column, feature, k):
    """
    compute top k frequent cell values and their counts.
    """
    if column.count() > 0:
        feature["most_common_raw_values"] = ordered_dict(column, k)


def compute_common_tokens(column, feature, k, feature_list):
    """
    compute top k frequent tokens and their counts.
    currently: tokens separated by white space
    at the same time, count on tokens which contain number(s)
    e.g. "$100", "60F", "123-456-7890"
    note: delimiter = " "
    """

    # if one of them specified, just compute all
    if (("most_common_tokens" in feature_list) or
            ("number_of_tokens_containing_numeric_char" in feature_list) or
            ("ratio_of_tokens_containing_numeric_char" in feature_list)):

        token = np.asarray([token for lst in column.str.split().dropna() for token in lst])
        if token.size:
            feature["most_common_tokens"] = ordered_dict2(token, k)
            cnt = sum([any(char.isdigit() for char in c) for c in token])
            if cnt > 0:
                feature["number_of_tokens_containing_numeric_char"] = cnt
                feature["ratio_of_tokens_containing_numeric_char"] = float(cnt) / token.size


def compute_common_tokens_by_puncs(column, feature, k, feature_list):
    """
    tokens seperated by all string.punctuation characters:
    '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
    """
    # if one of them specified, just compute all
    if (("most_common_tokens_split_by_punctuation" in feature_list) or
            ("number_of_distinct_tokens_split_by_punctuation" in feature_list) or
            ("ratio_of_distinct_tokens_split_by_punctuation" in feature_list) or
            ("number_of_tokens_split_by_punctuation_containing_numeric_char" in feature_list) or
            ("ratio_of_tokens_split_by_punctuation_containing_numeric_char" in feature_list)):

        col = column.dropna().values
        token_nested = [("".join((word if word.isalnum() else " ") for word in char).split()) for char in col]
        token = np.array([item for sublist in token_nested for item in sublist])
        if token.size:
            feature["most_common_tokens_split_by_punctuation"] = ordered_dict2(token, k)
            dist_cnt = np.unique(token).size
            feature["number_of_distinct_tokens_split_by_punctuation"] = dist_cnt
            feature["ratio_of_distinct_tokens_split_by_punctuation"] = float(dist_cnt) / token.size
            cnt = sum([any(char.isdigit() for char in c) for c in token])
            if cnt > 0:
                feature["number_of_tokens_split_by_punctuation_containing_numeric_char"] = cnt
                feature["ratio_of_tokens_split_by_punctuation_containing_numeric_char"] = float(cnt) / token.size


def compute_numeric_density(column, feature):
    """
    compute overall density of numeric characters in the column.
    """
    col = column.dropna().values
    if col.size:
        density = np.array([(sum(char.isdigit() for char in c), len(c)) for c in col])
        digit_total = density.sum(axis=0)
        feature["numeric_char_density"] = {'mean': float(digit_total[0]) / digit_total[1]}


def compute_contain_numeric_values(column, feature, feature_list):
    """
    caculate # and ratio of cells in the column which contains numbers.
    """
    # if one of them specified, just compute all
    if (("number_of_values_containing_numeric_char" in feature_list) or
            ("ratio_of_values_containing_numeric_char" in feature_list)):

        contain_digits = lambda x: any(char.isdigit() for char in x)
        cnt = column.dropna().apply(contain_digits).sum()
        if cnt > 0:
            feature["number_of_values_containing_numeric_char"] = cnt
            feature["ratio_of_values_containing_numeric_char"] = float(cnt) / column.count()

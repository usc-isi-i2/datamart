from builtins import range
import pandas as pd  # type: ignore
from langdetect import detect
from datamart.profilers.helpers.feature_compute_hih import is_Decimal_Number
import string
import numpy as np  # type: ignore
import re


# till now, this file totally compute 16 types of features

def compute_missing_space(column, feature, feature_list):
    """
    NOTE: this function may change the input column. It will trim all the leading and trailing whitespace.
            if a cell is empty after trimming(which means it only contains whitespaces),
            it will be set as NaN (missing value), and both leading_space and trailing_space will += 1.

    (1). trim and count the leading space and trailing space if applicable.
        note that more than one leading(trailing) spaces in a cell will still be counted as 1.
    (2). compute the number of missing value for a given series (column); store the result into (feature)
    """

    # if one of them is specified, just compute all; since does not increase lot computations
    if (("number_of_values_with_leading_spaces" in feature_list) or
            ("ratio_of_values_with_leading_spaces" in feature_list) or
            ("number_of_values_with_trailing_spaces" in feature_list) or
            ("ratio_of_values_with_trailing_spaces" in feature_list)):

        leading_space = 0
        trailing_space = 0

        for id, cell in column.iteritems():
            if (pd.isnull(cell)):
                continue

            change = False
            trim_leading_cell = re.sub(r"^\s+", "", cell)
            if (trim_leading_cell != cell):
                leading_space += 1
                change = True
            trim_trailing_cell = re.sub(r"\s+$", "", trim_leading_cell)
            if ((trim_trailing_cell != trim_leading_cell) or len(trim_trailing_cell) == 0):
                trailing_space += 1
                change = True

            # change the origin value in data
            if change:
                if (len(trim_trailing_cell) == 0):
                    column[id] = np.nan
                else:
                    column[id] = trim_trailing_cell

        feature["number_of_values_with_leading_spaces"] = leading_space
        feature["ratio_of_values_with_leading_spaces"] = leading_space / column.size
        feature["number_of_values_with_trailing_spaces"] = trailing_space
        feature["ratio_of_values_with_trailing_spaces"] = trailing_space / column.size


def compute_length_distinct(column, feature, delimiter, feature_list):
    """
    two tasks because of some overlaping computation:

    (1). compute the mean and std of length for each cell, in a given series (column);
        mean and std precision: 5 after point
        missing value (NaN): treated as does not exist
    (2). also compute the distinct value and token:
        number: number of distinct value or tokens, ignore the NaN
        ratio: number/num_total, ignore all NaN
    """
    # (1)
    column = column.dropna()  # get rid of all missing value
    if (column.size == 0):  # if the column is empty, do nothing
        return

    # 1. for character
    # lenth_for_all =  column.apply(len)
    # feature["string_length_mean"] = lenth_for_all.mean()
    # feature["string_length_std"] = lenth_for_all.std()

    # (2)
    if (("number_of_distinct_values" in feature_list) or
            ("ratio_of_distinct_values" in feature_list)):
        feature["number_of_distinct_values"] = column.nunique()
        feature["ratio_of_distinct_values"] = feature["number_of_distinct_values"] / float(column.size)

    if (("number_of_distinct_tokens" in feature_list) or
            ("ratio_of_distinct_tokens" in feature_list)):
        tokenlized = pd.Series([token for lst in column.str.split().dropna() for token in lst])  # tokenlized Series
        lenth_for_token = tokenlized.apply(len)
        # feature["token_count_mean"] = lenth_for_token.mean()
        # feature["token_count_std"] = lenth_for_token.std()
        feature["number_of_distinct_tokens"] = tokenlized.nunique()
        feature["ratio_of_distinct_tokens"] = feature["number_of_distinct_tokens"] / float(tokenlized.size)


def compute_lang(column, feature):
    """
    compute which language(s) it use for a given series (column); store the result into (feature).
    not apply for numbers

    PROBLEMS:
    1. not accurate when string contains digits
    2. not accurate when string is too short
    maybe need to consider the special cases for the above conditions
    """
    column = column.dropna()  # ignore all missing value
    if (column.size == 0):  # if the column is empty, do nothing
        return

    feature["natural_language_of_feature"] = list()
    language_count = {}

    for cell in column:
        if cell.isdigit() or is_Decimal_Number(cell):
            continue
        else:
            # detecting language
            try:
                language = detect(cell)
                if language in language_count:
                    language_count[language] += 1
                else:
                    language_count[language] = 1
            except Exception as e:
                print("there is something may not be any language nor number: {}".format(cell))
                pass

    languages_ordered = sorted(language_count, key=language_count.get, reverse=True)
    for lang in languages_ordered:
        lang_obj = {}
        lang_obj['name'] = lang
        lang_obj['count'] = language_count[lang]
        feature["natural_language_of_feature"].append(lang_obj)


def compute_filename(column, feature):
    """
    compute number of cell whose content might be a filename
    """
    column = column.dropna()  # ignore all missing value

    filename_pattern = r"^\w+\.[a-z]{1,5}"
    column.str.match(filename_pattern)
    num_filename = column.str.match(filename_pattern).sum()
    feature["num_filename"] = num_filename


def compute_punctuation(column, feature, weight_outlier):
    """
    compute the statistical values related to punctuations, for details, see the format section of README.

    not apply for numbers (eg: for number 1.23, "." does not count as a punctuation)

    weight_outlier: = number_of_sigma in function "helper_outlier_calcu"
    """

    column = column.dropna()  # get rid of all missing value
    if (column.size == 0):  # if the column is empty, do nothing
        return

    number_of_chars = sum(column.apply(len))  # number of all chars in column
    num_chars_cell = np.zeros(column.size)  # number of chars for each cell
    puncs_cell = np.zeros([column.size, len(string.punctuation)],
                          dtype=int)  # (number_of_cell * number_of_puncs) sized array

    # step 1: pre-calculations
    cell_id = -1
    for cell in column:
        cell_id += 1
        num_chars_cell[cell_id] = len(cell)
        # only counts puncs for non-number cell
        if cell.isdigit() or is_Decimal_Number(cell):
            continue
        else:
            counts_cell_punc = np.asarray(list(cell.count(c) for c in string.punctuation))
            puncs_cell[cell_id] = counts_cell_punc

    counts_column_punc = puncs_cell.sum(axis=0)  # number of possible puncs in this column
    cell_density_array = puncs_cell / num_chars_cell.reshape([column.size, 1])
    puncs_density_average = cell_density_array.sum(axis=0) / column.size

    # step 2: extract from pre-calculated data
    # only create this feature when punctuations exist
    if (sum(counts_column_punc) > 0):
        feature["most_common_punctuations"] = list()  # list of dict

        # extract the counts to feature, for each punctuation
        for i in range(len(string.punctuation)):
            if (counts_column_punc[i] == 0):  # if no this punctuation occur in the whole column, ignore
                continue
            else:
                punc_obj = {}
                punc_obj["punctuation"] = string.punctuation[i]
                punc_obj["count"] = counts_column_punc[i]
                punc_obj["ratio"] = counts_column_punc[i] / float(number_of_chars)
                punc_obj["punctuation_density_aggregate"] = {"mean": puncs_density_average[i]}
                # calculate outlier
                outlier_array = helper_outlier_calcu(cell_density_array[:, i], weight_outlier)
                # only one element in outlier
                punc_obj["punctuation_density_outliers"] = [{"n": weight_outlier,
                                                             "count": sum(outlier_array)}]
                feature["most_common_punctuations"].append(punc_obj)

    # step 3: sort
    feature["most_common_punctuations"] = sorted(feature["most_common_punctuations"], key=lambda k: k['count'],
                                                 reverse=True)


def helper_outlier_calcu(array, number_of_sigma):
    """
    input: array is a 1D numpy array, number_of_sigma is a integer.
    output: boolean array, size same with input array; true -> is outlier, false -> not outlier
    outlier def:
        the values that not within mean +- (number_of_sigma * sigma) of the statics of the whole list
    """
    mean = np.mean(array)
    std = np.std(array)
    upper_bound = mean + number_of_sigma * std
    lower_bound = mean - number_of_sigma * std
    outlier = (array > upper_bound) + (array < lower_bound)
    return outlier

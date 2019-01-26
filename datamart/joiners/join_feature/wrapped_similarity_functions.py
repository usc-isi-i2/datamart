from rltk.similarity import *


def to_lower(x: list):
    return [_.lower() for _ in x]


def to_set(x: list, ignore_cases: bool=True):
    if ignore_cases:
        return set([_.lower() for _ in x])
    return set(x)


def to_str(x: list, joiner: str=' '):
    return joiner.join(x)


def jaccard_sim(left: list, right: list):
    return jaccard_index_similarity(to_set(left), to_set(right))


def hybrid_jaccard_sim(left: list, right: list):
    return hybrid_jaccard_similarity(to_set(left), set(right))


def levenshtein_sim(left: list, right: list):
    return levenshtein_similarity(to_str(left), to_str(right))


def jaro_winkler_sim(left: list, right: list):
    return jaro_winkler_similarity(to_str(left), to_str(right))


def ngram_sim(left: list, right: list):
    return ngram_similarity(to_str(left), to_str(right))


def cosine_sim(left: list, right: list):
    return cosine_similarity(left, right)
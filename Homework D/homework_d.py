#! /usr/bin/env/python3
# Authored by : Christopher Lang
# Datetime    : 2017-03-03 10:26AM
# Python v.   : Python 3.5.2 64-bit Windows 10
import os
import sys
import joblib
import json
import re
from collections import defaultdict


def mapper(list_of_textlines):
    """Basic text clean and creating list of tokens

    Really basic cleaning. One big one missing is stopwords

    Arg(s):
        list_of_textlines (list(str)):
            A list of strings to gather tokens

    Return(s) (list(str)):
        Each string is a token found in document
    """
    text = [i.lower() for i in list_of_textlines]
    text = [re.subn("\s+|\n+", " ", i)[0] for i in text]
    text = [re.subn("[.!@#$%^&*()-_+=,./?\"'|\}{:;]+", " ", i)[0] for i in text]
    text = [re.split("\s+", i) for i in text]
    text = [[i for i in j if i != ''] for j in text]
    text = [i for i in text if len(i) > 0]
    text = [item for sublist in text for item in sublist]

    return text


def partition(list_of_tokens):
    """ Creates a frequency count for provided tokens

    Arg(s):
        list_of_tokens (list(str)):
            List of tokens

    Return(s) (dict):
        Each key is the term, its value is frequency count
    """
    freq_count = defaultdict(int)
    for word in list_of_tokens:
        freq_count[word] += 1

    return freq_count


def reducer(list_freq_count):
    """ Combines multiple frequency dictionaries into one

    Arg(s):
        list_freq_count (list(dict)):
            List of frequency dictionaries to combine. Their term frequencies
            is whats combined

    Return(s) (tuple(str, int)):
        Combined term frequency tuples
    """
    reduced_freq_count = defaultdict(int)

    for a_freq_count in list_freq_count:
        for a_freq_pair in a_freq_count:
            reduced_freq_count[a_freq_pair] += a_freq_count[a_freq_pair]

    result = reduced_freq_count.items()
    result = [i for i in result]

    result.sort(key=lambda x: x[1], reverse=True)

    return result


if __name__ == "__main__":
    wdir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(wdir)

    with open('config/homeworkd_config.json', 'r') as f:
        cfg = json.load(f)

    with open(cfg['dataloc'], 'r') as f:
        textdata = f.readlines()

    cleaned_text = mapper(textdata)
    cleaned_text = [partition(cleaned_text)]
    cleaned_text = reducer(cleaned_text)

    # Ran out of time for parallel versions. Would've used joblib for
    # embarassingly parallel work
    # Or used multiprocessing directly. The main part is to parallelize the
    # mapper function by distributing each line roughly evenly amongst the CPU
    # cores, or number of processes, you want
    # The results of mapper is again parallelized with partition function
    # And reduce, in this case, will actually be single threaded to combine
    # all frq count

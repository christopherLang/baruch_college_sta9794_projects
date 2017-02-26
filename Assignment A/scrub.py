import sys
import os
import csv
import logging as lg
import itertools as it
import multiprocessing as mp
import tqdm
import ConfigParser
from joblib import Parallel, delayed

# print wdir
# print "/".join([os.getcwd(), "..", "lib"])


def row_clean(rows):
    for a_row in rows:
        a_row[1:3] = [float(a_row[1]), int(a_row[2])]

    rows.sort(key=lambda x: x[0])

    result = [tuple(i) for i in rows]

    return result


def detect_noise(rows):
    noise_indices = set()
    result = dict()

    result['noise_rows'] = None
    result['n_duplicates'] = 0
    result['n_wrongLength'] = 0
    result['n_negativeNum'] = 0

    for i in range(len(rows)):

        if i > 1:
            # Starting from row 2, check for duplicates. Probably very slow
            # TODO - Find a way to avoid constantly check if we're starting
            #        from row 1 or above
            if rows[i][:-1] == rows[i - 1][:-1]:
                # This check if current row is the same as before. This only
                # works if row objects are immutable. Therefore, before running
                # detect_noise, row_clean must be run as it converts to tuples
                #
                # This also assumings that rows are sorted by time in ascending
                # order
                noise_indices.add(rows[i][-1])
                result['n_duplicates'] += 1

        # Presuming all rows should have 3 elements, one for each column
        if len(rows[i]) != 4:
            noise_indices.add(rows[i][-1])
            result['n_wrongLength'] += 1

        # Look for negative "price" (index 1) and "units traded" (index 2)
        elif rows[i][1] < 0 or rows[i][2] < 0:
            noise_indices.add(rows[i][-1])
            result['n_negativeNum'] += 1

    # If no noise rows are found based on the above logic, then return None
    if len(noise_indices) != 0:
        result['noise_rows'] = noise_indices

    return result


if __name__ == "__main__":
    wdir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(wdir)

    sys.path.append("/".join([os.getcwd(), "..", "lib"]))

    import utils

    # Settings
    configs = ConfigParser.ConfigParser()
    configs.read("config/assignmentA_config.ini")
    block_height = configs.getint('chunkOptions', 'blockHeight')
    n_cores = configs.getint('computeOptions', 'num_process')

    if n_cores is 0:
        n_cores = mp.cpu_count()

    else:
        n_cores = 1 if n_cores < 2 else n_cores

    print("Number of cores: " + str(mp.cpu_count()) + " (" + str(n_cores) + " processes requested)")
    print("Block height: " + str(block_height))
    print("Increments: " + str(window_inc))

    nrows_parsed = 0

    # ../Archive/data-big.txt
    with open("../Archive/data-big.txt", "rb") as f:
        reader = csv.reader(f)
        row_counter = 0
        # file_chunk = utils.simple_idchunker(reader, chunk_size=block_height)
        file_chunk = utils.simple_chunker(reader, chunk_size=block_height)

        noise_results = list()
        for rows in tqdm.tqdm(file_chunk):
            nrows = len(rows)
            row_index = range(row_counter, nrows)

            row_and_index = list()
            for a_row, index in zip(rows, row_index):
                row_and_index.append(a_row + [index])

            rows = row_clean(row_and_index)
            noise_results.append(detect_noise(rows))

            row_counter = nrows

    print("# of noise rows detected: " + str(sum([len(i['noise_rows']) for i in noise_results if i['noise_rows'] is not None])))
    print("    Due to duplicates   : " + str(sum([i['n_duplicates'] for i in noise_results])))
    print("    Due to wrong length : " + str(sum([i['n_wrongLength'] for i in noise_results])))
    print("    Due to neg. numbers : " + str(sum([i['n_negativeNum'] for i in noise_results])))
    # print(nrows_parsed)

import sys
import os
import csv
import logging as lg
import multiprocessing as mp
import tqdm
import ConfigParser
from joblib import Parallel, delayed

# print wdir
# print "/".join([os.getcwd(), "..", "lib"])


def row_clean(rows):
    rows.sort(key=lambda x: x[0])

    for a_row in rows:
        a_row[1:] = [float(a_row[1]), int(a_row[2])]


# def row_clean2(rows):
#     return [[relem[0], float(relem[1]), int(relem[2])] for relem in rows]


if __name__ == "__main__":
    wdir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(wdir)

    sys.path.append("/".join([os.getcwd(), "..", "lib"]))

    import utils

    # Settings
    configs = ConfigParser.ConfigParser()
    configs.read("config/assignmentA_config.ini")
    block_height = configs.getint('chunkOptions', 'blockHeight')
    window_inc = configs.getint('chunkOptions', 'window_increment')
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

        file_chunk = utils.sliding_chunker(reader, chunk_size=block_height,
                                           inc=window_inc)

        # Serial iteration
        # for rows in file_chunk:
        #     # r = row_clean2(rows)
        #     row_clean(rows)
        #     nrows_parsed += len(rows)

        # Embarrassingly parallel iteration
        with Parallel(n_jobs=n_cores, verbose=5) as par:
            par(delayed(row_clean)(row) for row in file_chunk)

    # print(nrows_parsed)

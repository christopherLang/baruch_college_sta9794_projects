import sys
import os
import csv
import logging as lg
import multiprocessing as mp
import tqdm
import ConfigParser

wdir = os.path.dirname(os.path.realpath(__file__))
os.chdir(wdir)

sys.path.append("/".join([os.getcwd(), "..", "lib"]))

import utils

print wdir
print "/".join([os.getcwd(), "..", "lib"])

# Settings
configs = ConfigParser.ConfigParser()
configs.read("config/assignmentA_config.ini")
chunk_size = configs.getint('readOptions', 'chunkSize')
block_height = configs.getint('readOptions', 'blockHeight')
nrows_parsed = 0


if __name__ == "__main__":
    with open("../Archive/data-big.txt", "rb") as f:
        reader = csv.reader(f)

        file_chunk = utils.row_chunker(reader, chunk_size=chunk_size)

        for row in file_chunk:
            r = [[relem[0], float(relem[1]), int(relem[2])] for relem in row]
            nrows_parsed += len(r)

    print(nrows_parsed)

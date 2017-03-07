import sys
import os
import csv
# import logging as lg
# import itertools as it
import multiprocessing as mp
import tqdm
import json
import datetime as dt
from mpi4py import MPI
from mpi4py.MPI import ANY_SOURCE
import numpy as np
import math

# print wdir
# print "/".join([os.getcwd(), "..", "lib"])

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


# def producer(comm, n, nprocess):
#     collection = utils.even_split(range(n), size - 1)

#     for pid, work in zip(range(1, size), collection):
#         comm.send(work, pid)


# def consumer(comm, root=0):
#     work_data = comm.recv(source=root)

#     print("Rank: " + str(rank))
#     print("N numbers: " + str(len(work_data)))


if __name__ == "__main__":
    # Set working directory to script's directory
    wdir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(wdir)

    sys.path.append("/".join([os.getcwd(), "..", "lib"]))

    # import utils
    import chunkers as chk
    import utils

    if rank == 0:
        # Master process, distribute data to the workers
        process_id = 1
        max_workerid = size - 1

        nrows = chk.get_nrows("../Archive/data-big.txt")

        print("# rows: " + str(nrows))
        print("Size: " + str(size))
        print(utils.size_sequencer(nrows, size - 1))

    #     nrows = chk.get_nrows("data.txt")
    #     print("# rows: " + str(nrows))

    #     with open("data.txt", "rb") as inputfile:
    #         data_reader = csv.reader(inputfile)
    #         data_chunker = chk.simple_chunker(data_reader, chunk_size=1000)

    #         for i in data_chunker:
    #             comm.isend(i, dest=process_id)
    #             print(process_id)
    #             process_id += 1

    #             if process_id > max_workerid:
    #                 process_id = 1

    # else:
    #     # Worker process, does stuff received from master process
    #     work_data = comm.irecv(source=0)

    #     print("Rank: " + str(rank))
    #     print("N numbers: " + str(len(work_data)))

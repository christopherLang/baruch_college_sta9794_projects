import sys
import os
import json
import datetime as dt
from mpi4py import MPI
import ntpath
import numpy as np

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


def worker(file, row_index, rank, delimiter=",", logger=None,
           enable_debug=False):
    start_row = row_index[0]
    nrows_read = row_index[1] - row_index[0] + 1
    reader = chk.row_reader(file, start_row, nrows_read)

    if logger is not None and enable_debug:
            msg = "rank{0} start index: {1}, nrows: {2}"
            msg = msg.format(rank, start_row, nrows_read)
            logger.debug(msg)

    nrows_parsed = 0
    rows = list()
    for a_row in reader:
        nrows_parsed += 1
        rows.append(a_row)

    if "\r\n" in rows[0] and "\r\n" in rows[-1]:
        rows = [i.split("\r\n")[0] for i in rows]

        if logger is not None and enable_debug:
                msg = "\\r\\n end of line detected"
                logger.debug(msg)

    elif "\n" in rows[0] and "\n" in rows[-1]:
        rows = [i.split("\n")[0] for i in rows]

        if logger is not None and enable_debug:
                msg = "\\n end of line detected"
                logger.debug(msg)

    rows = [i.split(delimiter) for i in rows]

    rows = row_clean(rows, start_row, start_row + nrows_read)

    with open(noiseloc, "r") as f:
        noise_index = f.readlines()
        noise_index = [int(i) for i in noise_index]

    noise_index = set(noise_index)

    nrows = len(rows)
    rows = remove_noise(rows, noise_index)

    nrows_denoise = len(rows)

    result = {
        "is_normal": is_prices_normal(rows),
        "nrows_org": nrows,
        "nrows_denoised": nrows_denoise
    }

    return result


def row_clean(rows, start_index, end_index):
    for a_row, row_index, in zip(rows, range(start_index, end_index)):
        a_row[1:3] = [float(a_row[1]), int(a_row[2])]
        a_row += [row_index]

    rows.sort(key=lambda x: x[0])

    result = [tuple(i) for i in rows]

    return result


def remove_noise(rows, noise_indices):
    rows = [i for i in rows if i[-1] not in noise_indices]

    return rows


def is_prices_normal(rows):
    prices = [i[1] for i in rows]

    result = mutils.jarque_bera(prices)

    return result


if __name__ == "__main__":
    # Set working directory to script's directory
    wdir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(wdir)

    sys.path.append("/".join([os.getcwd(), "..", "lib"]))

    import chunkers as chk
    import ResultLogger as rl
    import utils
    import math_utils as mutils
    import logging

    # Load program settings
    with open("config/assignmentA_config.json", "r") as f:
        configs = json.load(f)

    nchunk = configs['chunk_size']
    n_cores = configs['num_process']
    row_delim = configs['col_delimiter']
    enable_debug = configs['enable_debug']
    noiseloc = configs['noisefileloc']
    exec_logloc = configs['exec_logloc'].format("normal")
    result_logloc = configs['result_logloc'].format("normal")

    # Create execution logger
    # ---------------------------------------------------------------------
    if enable_debug:
        log_level = logging.DEBUG

    else:
        log_level = logging.INFO

    lg = logging.getLogger()
    lgr_handler = logging.FileHandler(exec_logloc)
    lgr_fmt = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    lgr_handler.setFormatter(lgr_fmt)
    lg.addHandler(lgr_handler)
    lg.setLevel(log_level)

    # Get the location of the data file to be parsed
    # -------------------------------------------------------------------------
    dataloc = None
    try:
        dataloc = sys.argv[1]

    except IndexError:
        # No data loc was provided via command line, check config file
        pass

    if dataloc is None:
        try:
            dataloc = configs['dataloc']

        except KeyError:
            # no dataloc was provided in configuration file
            pass

    if dataloc is None:
        raise Exception("No data location was provided")

    if rank == 0:
        # Master process section ----------------------------------------------
        # ---------------------------------------------------------------------

        # Create time tracker -------------------------------------------------
        # ---------------------------------------------------------------------
        import TimeTrack
        tt = TimeTrack.TimeTrack()

        # Create result logger
        # ---------------------------------------------------------------------
        result_log = rl.ResultLogger("result/noise_testresult.txt",
                                     configs['prog_title'])

        nrows = chk.get_nrows(dataloc)

        print("MPI size: " + str(size))
        print("Chunk size: " + str(nchunk))

        result_log.init_section("Program Information", level=0)
        result_log.add_section_kv("MPI size", size)
        result_log.add_section_kv("Chunk size", nchunk)
        result_log.add_section_kv("File name", ntpath.basename(dataloc))
        result_log.add_section_kv("Row count", nrows)
        result_log.exec_section()

        if size == 1:
            lg.info("MPI size {0}, working in serial".format(size))

        else:
            lg.info("MPI size {0}, working in parallel".format(size))

        s_index = 0
        nrows_left = nrows
        nworkers = size

        if nworkers < 1:
            nworkers = 1

        scrub_results = list()

        worker_row_indices = list()
        for i in range(nworkers):
            worker_row_indices.insert(i, [])

        while nrows_left != 0:
            row_indices = utils.size_sequencer(nchunk, nworkers, s_index)

            for a_row_indices in range(len(row_indices)):
                index_interval = row_indices[a_row_indices]
                worker_row_indices[a_row_indices].append(index_interval)

            nrows_left -= nchunk
            s_index += nchunk

            if nchunk > nrows_left:
                nchunk = nrows_left

    else:
        worker_row_indices = None

    if worker_row_indices is not None:
        print(len(worker_row_indices))

    row_indices = comm.scatter(worker_row_indices, root=0)

    if rank == 0:
        extt = TimeTrack.TimeTrack()

    work_result = list()
    for index_interval in row_indices:
        r = worker(dataloc, index_interval, rank, row_delim, lg, enable_debug)
        work_result.append(r)

    scrub_results = comm.gather(work_result, root=0)

    if rank == 0:
        extt.pause_time()
        tt.pause_time()

        scrub_results = [item for sublist in scrub_results for item in sublist]

        r = dict()
        r['n_normal_true'] = []
        r['all_true'] = False
        r['nrows_parsed'] = 0
        r['nrows_denoised'] = 0

        for a_result in scrub_results:
            r['n_normal_true'].append(a_result['is_normal'])
            r['nrows_parsed'] += a_result['nrows_org']
            r['nrows_denoised'] += a_result['nrows_denoised']

        r['all_true'] = all(r['n_normal_true'])
        r['n_normal_true'] = [1 for i in r['n_normal_true'] if i is True]
        n_not_true = np.sum([1 for i in r['n_normal_true'] if i is False])
        r['n_normal_true'] = round(np.sum(r['n_normal_true']), 1)

        result_log.init_section("Analysis Output", level=0)
        result_log.add_section_kv("Execution start time",
                                  tt.start_time_pretty())
        result_log.add_section_kv("Execution end time", tt.end_time_pretty())
        result_log.add_section_kv("Execution elapsed time",
                                  tt.elapsed_pretty())
        result_log.add_section_kv("Row parse elapsed time",
                                  extt.elapsed_pretty())
        result_log.add_section_kv("Row count", r['nrows_parsed'])
        result_log.add_section_kv("Row count (denoised)", r['nrows_denoised'])
        result_log.add_section_kv("All normal", r['all_true'])
        result_log.add_section_kv("# chunks not normal", n_not_true)

        velocity = r['nrows_parsed'] / extt.elapsed_seconds()
        velocity = round(velocity, 2)
        result_log.add_section_kv("Velocity (rows parsed / sec)", velocity)
        # result_log.add_section_kv("Noise rows (negative num.)",
        #                           r['n_negativeNum'])
        # result_log.add_section_kv("Noise rows (# of columns)",
        #                           r['n_wrongLength'])
        # result_log.add_section_kv("Noise rows (timestamp format)",
        #                           r['n_timestampFormat'])

        result_log.exec_section()

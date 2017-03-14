import sys
import os
import json
import datetime as dt
from mpi4py import MPI
import re
import ntpath
import numpy as np


def worker(rows, noisefile, row_index, rank, execlogger, noiserow_check,
           delimiter=","):
    start_row = row_index[0]
    nrows_read = row_index[1] - row_index[0] + 1
    # reader = chk.row_reader(dataloc, start_row, nrows_read)
    # rows = [a_row for a_row in reader]
    nrows_parsed = len(rows)

    msg = "rank-{0} start index: {1}, nrows: {2}"
    msg = msg.format(rank, start_row, nrows_read)
    execlogger.debug(msg)

    row_indices = xrange(start_row + 1, start_row + nrows_read + 1)
    indexed_rows = [(a_row, i) for a_row, i in zip(rows, row_indices)]

    noise_i = set(a_row[1] for a_row in indexed_rows
                  if noiserow_check.match(a_row[0]) is None)

    regex_noise = len(noise_i)
    msg = "rank-{0} found {1} noise rows with regex"
    msg = msg.format(rank, regex_noise)
    execlogger.debug(msg)

    indexed_rows.sort(key=lambda x: x[0])

    noise_i.update((indexed_rows[i][1] for i in range(1, len(indexed_rows))
                    if (indexed_rows[i][0] == indexed_rows[i - 1][0]) is True))

    dup_noise = len(noise_i) - regex_noise
    msg = "rank-{0} found {1} noise rows as duplicates"
    msg = msg.format(rank, dup_noise)
    execlogger.debug(msg)

    indexed_rows = [i for i in indexed_rows if i[1] not in noise_i]

    indices = np.array([i[1] for i in indexed_rows], dtype=np.int64)

    numeric_data = [i[0].split(delimiter)[1:] for i in indexed_rows]
    prices = np.array([i[0] for i in numeric_data], dtype=np.float64)
    units_traded = np.array([i[1] for i in numeric_data], dtype=np.int64)

    stdev = np.std(prices)
    price_mean = np.mean(prices)
    upp_stdev = 3 * stdev
    low_stdev = -3 * stdev

    prices_demeaned = prices - price_mean

    noise_i.update(indices[np.logical_or(prices_demeaned < low_stdev,
                                         prices_demeaned > upp_stdev)])

    price_sigma_noise = len(noise_i) - dup_noise
    msg = "rank-{0} found {1} noise rows as price sigma noise"
    msg = msg.format(rank, price_sigma_noise)
    execlogger.debug(msg)

    noise_i.update(indices[np.logical_and(prices > 0, units_traded == 0)])

    units_sigma_noise = len(noise_i) - price_sigma_noise
    msg = "rank-{0} found {1} noise rows as units traded noise"
    msg = msg.format(rank, units_sigma_noise)
    execlogger.debug(msg)

    msg = "rank-{0} detect noise finished, nrows {1} parsed, {2} detected"
    msg = msg.format(rank, nrows_parsed, len(noise_i))
    execlogger.info(msg)

    if len(noise_i) > 0:
        noisefile.writelines([str(i) + "\n" for i in noise_i])

        msg = "rank-{0} wrote {1} noise row indices to disk"
        msg = msg.format(rank, len(noise_i))
        execlogger.info(msg)

    result = dict()
    result['nrows'] = nrows_parsed
    result['n_noise'] = len(noise_i)

    return result


if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Set working directory to script's directory
    wdir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(wdir)

    sys.path.append("/".join([os.getcwd(), "..", "lib"]))

    import chunkers as chk
    import ResultLogger as rl
    import utils
    import logging
    import Timetrack

    # Create noise row checker regex searcher
    row_regex = [
        r"^\d{8}([:]\d{2}){3}[.]\d+",
        r"[1-9]+[0-9.]+",
        r"[0-9.]+\r?$"]

    row_regex = r"[,]".join(row_regex)

    noiserow_check = re.compile(row_regex)

    # Load program settings
    with open("config/scrub_config.json", "r") as f:
        cfg = json.load(f)

    row_delim = cfg['col_delimiter']
    enable_debug = cfg['enable_debug']
    noiseloc = cfg['noisefileloc']
    exec_logloc = cfg['exec_logloc']
    result_logloc = cfg['result_logloc']

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
    # lg.addHandler(logging.StreamHandler())
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
            dataloc = cfg['dataloc']

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
        tt = Timetrack.Timetrack()

        # Create result logger
        # ---------------------------------------------------------------------
        result_log = rl.ResultLogger(result_logloc, cfg['prog_title'])

        nrows = chk.get_nrows(dataloc)

        result_log.init_section("Program Information", level=0)
        kvs = list()
        kvs.append(("MPI size", size))
        kvs.append(("Chunk size", cfg['chunk_size']))
        kvs.append(("File name", ntpath.basename(dataloc)))
        kvs.append(("Row count", nrows))
        result_log.add_section_kvs(kvs)
        result_log.exec_section()

        if size == 1:
            lg.info("MPI size {0}, working in serial".format(size))

        else:
            lg.info("MPI size {0}, working in parallel".format(size))

        s_index = 0
        nchunk = cfg['chunk_size']
        nrows_left = nrows
        nworkers = size
        scrub_results = list()
        worker_row_indices = list()
        row_indices = list()
        for i in range(nworkers):
            worker_row_indices.insert(i, [])

        while nrows_left != 0:
            row_indices.append(utils.size_sequencer(nchunk, nworkers, s_index))

            nrows_left -= nchunk
            s_index += nchunk

            if nchunk > nrows_left:
                nchunk = nrows_left

        row_indices = [item for sublist in row_indices for item in sublist]
        row_indices = utils.even_split(row_indices, size)

        for ipair, wp in zip(row_indices, worker_row_indices):
            wp.extend(ipair)

    else:
        worker_row_indices = None

    if worker_row_indices is not None:
        print(len(worker_row_indices))

    row_indices = comm.scatter(worker_row_indices, root=0)

    if rank == 0:
        extt = Timetrack.Timetrack()

    # Create row reader object
    rowreader = chk.Rowread(dataloc, row_indices[0][0])
    # Open a file for workers to write noise row indicies
    filename = "noise-rank" + str(rank) + "-"
    filename += dt.datetime.strftime(dt.datetime.utcnow(),
                                     format="%Y%m%dT%H%M%S")
    filename = "cache/" + filename + ".txt"
    with open(filename, "w") as file:
        work_result = list()
        wtt = Timetrack.Timetrack()
        for index_interval in row_indices:
            rowreader.set_startrow(index_interval[0] + 1)
            rows = rowreader.read(index_interval[1] - index_interval[0] + 1)
            r = worker(rows, file, index_interval, rank, lg, noiserow_check,
                       row_delim)
            work_result.append(r)

            msg = "rank-{0} finished execution in {1} seconds"
            msg = msg.format(rank, round(wtt.elapsed_seconds(), 4))
            lg.info(msg)

            wtt.reset_time()

    scrub_results = comm.gather(work_result, root=0)

    if rank == 0:
        extt.pause_time()

        scrub_results = [item for sublist in scrub_results for item in sublist]
        # Total aggregate count
        r = dict()
        r['nrows'] = 0
        r['n_noise'] = 0

        for a_result in scrub_results:
            r['nrows'] += a_result['nrows']
            r['n_noise'] += a_result['n_noise']

        # Combine noise files
        noise = list()
        if os.path.exists(noiseloc) is True:
            os.remove(noiseloc)

        for a_file in os.listdir("cache"):
            with open("cache/" + a_file, "r") as cachenoisefile:
                with open(noiseloc, "a") as noisefile:
                    noisefile.writelines(cachenoisefile.readlines())

            os.remove("cache/" + a_file)

        tt.pause_time()

        result_log.init_section("Scrub Analysis Output", level=0)

        kvs = list()
        kvs.append(("Execution start time", tt.start_time_pretty()))
        kvs.append(("Execution end time", tt.end_time_pretty()))
        kvs.append(("Execution elapsed time", tt.elapsed_pretty()))
        kvs.append(("Row parse elapsed time", extt.elapsed_pretty()))
        kvs.append(("Row count", r['nrows']))
        kvs.append(("Total # noise rows", r['n_noise']))

        velocity = r['nrows'] / extt.elapsed_seconds()
        velocity = round(velocity, 2)
        kvs.append(("Velocity (rows parsed / sec)", velocity))

        result_log.add_section_kvs(kvs)
        result_log.exec_section()

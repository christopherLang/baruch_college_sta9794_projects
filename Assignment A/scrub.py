import sys
import os
import json
import datetime as dt
from mpi4py import MPI
import re
import ntpath
import numpy as np
import psutil


def worker(rows, noisefile, row_index, rank, execlogger, noiserow_check,
           delimiter=","):
    start_row = row_index[0]
    nrows_read = row_index[1] - row_index[0] + 1
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

    msg = "rank-{0} finished scrubbing in {1} seconds"
    msg = msg.format(rank, tt.elapsed_seconds(tag="scrub_time"))
    execlogger.info(msg)

    if len(noise_i) > 0:
        tt.new_time(tag="noise_piece_io")

        noisefile.writelines([str(i) + "\n" for i in noise_i])

        msg = "rank-{0} wrote {1} noise row indices to disk"
        msg = msg.format(rank, len(noise_i))
        execlogger.info(msg)

        time_io = tt.elapsed_seconds(tag='noise_piece_io')

    else:
        time_io = 0

    result = dict()
    result['nrows'] = nrows_parsed
    result['n_noise'] = len(noise_i)
    result['noiseio_elapsed'] = time_io

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

    # Create time tracker -----------------------------------------------------
    # -------------------------------------------------------------------------
    tt = Timetrack.Timetrack()

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
    signalloc = cfg['signalfileloc']
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
        # Master process section

        # Create result logger
        # ---------------------------------------------------------------------
        result_log = rl.ResultLogger(result_logloc, cfg['prog_title'])

        tt.new_time('get_nrow')
        nrows = chk.get_nrows(dataloc)
        tt.pause_time('get_nrow')

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

    row_indices = comm.scatter(worker_row_indices, root=0)

    # Create row reader object
    rowreader = chk.Rowread(dataloc, row_indices[0][0])
    # Create base filename for file pieces
    nfilename = "noise-rank" + str(rank) + "-"
    nfilename += dt.datetime.strftime(dt.datetime.utcnow(),
                                      format="%Y%m%dT%H%M%S")
    nfilename = "cache/" + nfilename + ".txt"

    with open(nfilename, "w") as nfile:
        work_result = list()
        tt.new_time(tag="scrub_time")
        for index_interval in row_indices:
            rowreader.set_startrow(index_interval[0] + 1)
            rows = rowreader.read(index_interval[1] -
                                  index_interval[0] + 1)

            msg = "rank-{0} read {1} rows, memory used: {2} GB"
            msg = msg.format(rank, len(rows),
                             round(psutil.virtual_memory()[3] * 1e-9), 2)
            lg.info(msg)

            r = worker(rows, nfile, index_interval, rank, lg,
                       noiserow_check, row_delim)
            work_result.append(r)

            msg = "rank-{0} finished execution in {1} seconds"
            msg = msg.format(rank, round(tt.elapsed_seconds(tag="scrub_time"),
                                         4))
            lg.info(msg)

        tt.pause_time(tag='scrub_time')

    scrub_results = comm.gather(work_result, root=0)

    if rank == 0:
        scrub_results = [item for sublist in scrub_results for item in sublist]
        r = utils.gather_dict(scrub_results)

        # Combine noise files
        tt.new_time(tag='noise_agg')
        if os.path.exists(noiseloc) is True:
            os.remove(noiseloc)

        cachefiles = os.listdir("cache")
        noisefiles = [i for i in cachefiles if i.startswith("noise")]
        for a_file in noisefiles:
            with open("cache/" + a_file, "r") as cachenoisefile:
                with open(noiseloc, "a") as noisefile:
                    noisefile.writelines(cachenoisefile.readlines())

            os.remove("cache/" + a_file)

        tt.pause_time(tag='noise_agg')

        tt.new_time(tag='signal_write')
        with open(noiseloc, 'r') as noisefile:
            noise_indices = noisefile.readlines()
            noise_indices = set(noise_indices)

        with open(signalloc, "w") as signalfile:
            for i in xrange(nrows):
                if i not in noise_indices:
                    signalfile.write(str(i) + "\n")

        tt.pause_time(tag='signal_write')

        tt.pause_time()

        result_log.init_section("Scrub Analysis Output", level=0)

        kvs = list()
        kvs.append(("Scrub program start time", tt.start_time_pretty()))
        kvs.append(("Scrub program end time", tt.end_time_pretty()))
        kvs.append(("Scrub program elapsed time",
                    tt.elapsed_pretty(ndig_secs=4)))

        scrubbing_time = tt.elapsed_seconds(tag='scrub_time')
        scrubbing_time -= r['noiseio_elapsed']
        kvs.append(("Row scrubbing elapsed time",
                    utils.pretty_time_string(seconds=scrubbing_time,
                                             ndig_secs=4)))
        kvs.append(("Row counting elapsed time",
                    tt.elapsed_pretty('get_nrow')))

        noiseio_time = tt.elapsed_seconds(tag='noise_agg')
        noiseio_time += r['noiseio_elapsed']
        kvs.append(('noise.txt IO elapsed time',
                    utils.pretty_time_string(seconds=noiseio_time,
                                             ndig_secs=4)))
        kvs.append(('signal.txt IO elapsed time',
                    tt.elapsed_pretty(tag='signal_write', ndig_secs=4)))
        kvs.append(("Row count", r['nrows']))
        kvs.append(("Total # noise rows", r['n_noise']))

        velocity = r['nrows'] / tt.elapsed_seconds(tag='scrub_time')
        velocity = round(velocity, 4)
        kvs.append(("Velocity (rows parsed / sec)", velocity))

        result_log.add_section_kvs(kvs)
        result_log.exec_section()

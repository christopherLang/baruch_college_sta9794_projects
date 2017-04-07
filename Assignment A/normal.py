import sys
import os
import json
from mpi4py import MPI
import ntpath
import numpy as np
from pprint import pprint as pp

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


if __name__ == "__main__":
    # Set working directory to script's directory
    wdir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(wdir)

    sys.path.append("/".join([os.getcwd(), "..", "lib"]))

    import chunkers as chk
    import ResultLogger as rl
    import utils
    import logging
    import Timetrack

    # Create time tracker -------------------------------------------------
    # ---------------------------------------------------------------------
    tt = Timetrack.Timetrack()

    # Load program settings
    with open("config/normal_config.json", "r") as f:
        cfg = json.load(f)

    nchunk = cfg['chunk_size']
    row_delim = cfg['col_delimiter']
    enable_debug = cfg['enable_debug']
    noiseloc = cfg['noisefileloc']
    delimiter = cfg['col_delimiter']

    # Create execution logger
    # ---------------------------------------------------------------------
    if enable_debug:
        log_level = logging.DEBUG

    else:
        log_level = logging.INFO

    lg = logging.getLogger()
    lgr_handler = logging.FileHandler(cfg['exec_logloc'].format("normal"))
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
            dataloc = cfg['dataloc']

        except KeyError:
            # no dataloc was provided in configuration file
            pass

    if dataloc is None:
        raise Exception("No data location was provided")

    if rank == 0:
        # Master process section ----------------------------------------------
        # ---------------------------------------------------------------------

        # Create result logger
        # ---------------------------------------------------------------------
        result_log = rl.ResultLogger(cfg['result_logloc'].format("normal"),
                                     cfg['prog_title'])

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

        tt.new_time(tag='compute_mean')

    else:
        worker_row_indices = None

    row_indices = comm.scatter(worker_row_indices, root=0)

    # Create row reader object
    rowreader = chk.Rowread(dataloc, row_indices[0][0])

    # Read in noise indices from scrub.py
    with open(noiseloc, "r") as f:
        noise_index = f.readlines()
        noise_index = set(int(i) for i in noise_index)

    price_mean = dict()
    price_mean['nrows'] = 0
    for i_interval in row_indices:
        rowreader.set_startrow(i_interval[0] + 1)
        rows = rowreader.read(i_interval[1] - i_interval[0] + 1)

        indexed_rows = [a_row for a_row in enumerate(rows, i_interval[0] + 1)]
        indexed_rows = [i for i in indexed_rows if i[0] not in noise_index]

        prices = (i[1].split(delimiter)[1] for i in indexed_rows)
        prices = np.fromiter(prices, dtype=np.float64)

        price_mean["total_price"] = np.sum(prices)
        price_mean["nprices"] = np.int64(prices.size)
        price_mean['nrows'] += np.int64(len(rows))

    partial_means = comm.gather(price_mean, root=0)

    if rank == 0:
        tt.pause_time(tag='compute_mean')
        price_mean = utils.gather_dict(partial_means)

        price_mean = price_mean['total_price'] / price_mean['nprices']

        packets = [{"mean": price_mean, "i": i} for i in worker_row_indices]

        tt.new_time(tag="compute_deviations")
    else:
        packets = None

    packet = comm.scatter(packets, root=0)

    rowreader.reset(row_indices[0][0] + 1)
    test_stats = list()
    for i_interval in packet['i']:
        rowreader.set_startrow(i_interval[0] + 1)
        rows = rowreader.read(i_interval[1] - i_interval[0] + 1)

        indexed_rows = [a_row for a_row in enumerate(rows, i_interval[0] + 1)]
        indexed_rows = [i for i in indexed_rows if i[0] not in noise_index]

        prices = (i[1].split(delimiter)[1] for i in indexed_rows)
        prices = np.fromiter(prices, dtype=np.float64)

        deviation = prices - packet['mean']
        pstats = dict()
        pstats['n'] = 0
        pstats['stdev_num'] = np.sum(np.power(deviation, 2))
        pstats['skew_num'] = np.sum(np.power(deviation, 3))
        pstats['kurt_num'] = np.sum(np.power(deviation, 4))
        pstats['n'] += np.int64(prices.size)

        test_stats.append(pstats)

        msg = "rank-{0} Start row: {1}, End row: {2}"
        msg = msg.format(rank, i_interval[0] + 1, i_interval[1] + 1)
        lg.debug(msg)

        msg = "rank-{0} nprices: {1}".format(rank, prices.size)
        lg.debug(msg)

        msg = "rank-{0} First price: {1}, dev: {2}".format(rank, prices[0],
                                                           deviation[0])
        lg.debug(msg)

        msg = "rank-{0} Last price: {1}, dev: {2}".format(rank, prices[-1],
                                                          deviation[-1])
        lg.debug(msg)

    test_stats = utils.gather_dict(test_stats)

    msg = "rank-{0} stdev numerator: {1}".format(rank, test_stats['stdev_num'])
    lg.info(msg)

    msg = "rank-{0} skew numerator: {1}".format(rank, test_stats['skew_num'])
    lg.info(msg)

    msg = "rank-{0} kurt numerator: {1}".format(rank, test_stats['kurt_num'])
    lg.info(msg)

    partial_stats = comm.gather(test_stats, root=0)

    if rank == 0:
        tt.pause_time(tag='compute_deviations')

        jarque_bera_stats = {
            "stdev_num": 0, "skew_num": 0, "kurt_num": 0, "n": 0
        }
        jarque_bera_stats = utils.gather_dict(partial_stats)
        final_stdev = jarque_bera_stats['stdev_num']
        final_stdev /= (jarque_bera_stats['n'] - 1)
        final_stdev = np.sqrt(final_stdev)

        final_skew = jarque_bera_stats['skew_num'] / jarque_bera_stats['n']
        final_skew /= np.power(final_stdev, 3)

        final_kurt = jarque_bera_stats['kurt_num'] / jarque_bera_stats['n']
        final_kurt /= np.power(final_stdev, 4)

        jb_chisq = jarque_bera_stats['n'] / np.float64(6)
        jb_chisq *= np.power(final_skew, 2) + (np.power(final_kurt - 3, 2))
        jb_chisq /= np.float64(4)

        tt.pause_time()

        lg.info("stdev: {0}, skew: {1}, kurt: {2}".format(final_stdev,
                                                          final_skew,
                                                          final_kurt))
        lg.info("Jarque-Bera test statistic: {0}".format(jb_chisq))
        lg.info("Null hypothesis: Series is likely normally distributed")

        msg = "significance 95% (JB Stat <= 5.99): {0}"
        msg = msg.format(jb_chisq <= np.float64(5.99))
        lg.info(msg)

        msg = "significance 99% (JB Stat <= 9.21): {0}"
        msg = msg.format(jb_chisq <= np.float64(9.21))
        lg.info(msg)

        result_log.init_section("Analysis Output", level=0)

        analysis_output = list()
        analysis_output.append(("Normal program start time",
                                tt.start_time_pretty()))
        analysis_output.append(("Normal program end time",
                                tt.end_time_pretty()))
        analysis_output.append(("Normal program elapsed time",
                                tt.elapsed_pretty()))

        stat_compute_time = (tt.elapsed_seconds(tag='compute_mean') +
                             tt.elapsed_seconds(tag='compute_deviations'))

        analysis_output.append(
            ("Test statistic compute time",
             utils.pretty_time_string(seconds=stat_compute_time))
        )
        # result_log.init_section("Analysis Output", level=0)

        velocity = jarque_bera_stats['n'] * 2
        velocity /= stat_compute_time
        velocity = str(round(velocity, 4))
        analysis_output.append(("Velocity (stat compute / sec)", velocity))

        analysis_output.append(("# of prices", str(jarque_bera_stats['n'])))
        analysis_output.append(("Mean (price)", str(round(price_mean, 4))))
        analysis_output.append(("STDEV (price)", str(round(final_stdev, 4))))
        analysis_output.append(("Skewness (price)", str(round(final_skew, 4))))
        analysis_output.append(("Kurtosis (price)", str(round(final_kurt, 4))))
        analysis_output.append(("Jarque-Bera test statistic",
                                str(round(jb_chisq, 4))))
        analysis_output.append(("Null hypothesis", "Price is normal"))
        analysis_output.append(("Compared to", "Chi-square, 2 dof"))
        analysis_output.append(("significance 95% (JB Stat <= 5.99)",
                                str(jb_chisq <= np.float64(5.99))))
        analysis_output.append(("significance 99% (JB Stat <= 9.21)",
                                str(jb_chisq <= np.float64(9.21))))

        result_log.add_section_kvs(analysis_output)
        result_log.exec_section()

import sys
import os
import csv
# import logging as lg
# import itertools as it
import multiprocessing as mp
import tqdm
import json
import datetime as dt

# print wdir
# print "/".join([os.getcwd(), "..", "lib"])


def worker(rows):
    nrows_parsed = len(rows)

    rows = row_clean(rows)
    rows = detect_noise(rows)

    result = dict()
    result['rows_parsed'] = nrows_parsed
    result['noise'] = rows

    return result


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

    import chunkers as chk
    import ResultLogger as rl
    import utils

    # Settings
    with open("config/assignmentA_config.json", "r") as f:
        configs = json.load(f)

    block_height = configs['blockHeight']
    n_cores = configs['num_process']

    # Get the location of the data file to be parsed
    # --------------------------------------------------------------------------
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

    # Create result logger
    # --------------------------------------------------------------------------
    result_log = rl.ResultLogger("result/testresult.txt",
                                 configs['prog_title'])

    if n_cores is 0:
        n_cores = mp.cpu_count()

    else:
        n_cores = 1 if n_cores < 2 else n_cores

    print("Number of cores: " + str(mp.cpu_count()) + " (" + str(n_cores) + " processes requested)")
    print("Block height: " + str(block_height))

    prog_info = list()
    prog_info.append(("# of CPU cores", str(mp.cpu_count())))
    prog_info.append(("# of cores requested", str(n_cores)))
    prog_info.append(("Block height", str(block_height)))

    prog_info = result_log.kv_format(prog_info)

    result_log.add_lines(result_log.section("Program Information", prog_info))

    nrows_parsed = 0
    start_time = dt.datetime.today()
    # ../Archive/data-big.txt
    with open("data.txt", "rb") as f:
        reader = csv.reader(f)
        current_row = 0
        nrows_parsed = 0
        file_chunk = chk.simple_chunker(reader, chunk_size=block_height)

        noise_results = list()
        for rows in tqdm.tqdm(file_chunk):
            nrows_parsed += len(rows)
            nrows = len(rows)
            row_index = range(current_row, nrows)

            row_and_index = list()
            for a_row, index in zip(rows, row_index):
                row_and_index.append(a_row + [index])

            work_result = worker(row_and_index)

            noise_results.append(work_result['noise'])
            nrows_parsed += work_result['rows_parsed']

            current_row = nrows

    end_time = dt.datetime.today()
    # Gather work results
    analysis_results = dict()

    analysis_results['nrows_noise'] = (
        [len(i['noise_rows']) for i in noise_results
         if i['noise_rows'] is not None]
    )
    analysis_results['nrows_noise'] = sum(analysis_results['nrows_noise'])

    analysis_results['dupe'] = sum([i['n_duplicates'] for i in noise_results])
    analysis_results['wrg'] = sum([i['n_wrongLength'] for i in noise_results])
    analysis_results['neg'] = sum([i['n_negativeNum'] for i in noise_results])

    exec_time_result = utils.execution_time(start_time, end_time)

    analysis_log = list()

    analysis_log.append(("Execution start time", exec_time_result['start']))
    analysis_log.append(("Execution end time", exec_time_result['end']))

    analysis_log.append(("Elapsed time", exec_time_result['pretty_str']))

    velocity = nrows_parsed / exec_time_result['seconds']
    velocity = round(velocity, 2)

    analysis_log.append(("Rows parsed", str(nrows_parsed)))
    analysis_log.append(("Velocity (rows parsed / sec)", str(velocity)))

    analysis_log.append(("Total # noise rows",
                         str(analysis_results['nrows_noise'])))
    analysis_log.append(("Noise rows (duplicates)",
                         str(analysis_results['dupe'])))
    analysis_log.append(("Noise rows (negative num.)",
                         str(analysis_results['neg'])))
    analysis_log.append(("Noise rows (timestamp length)",
                         str(analysis_results['wrg'])))
    analysis_log = result_log.kv_format(analysis_log)

    analysis_log = result_log.section("Analysis Output", analysis_log)

    result_log.add_lines(analysis_log)

    print("# of noise rows detected: " + str(analysis_results['nrows_noise']))
    print("    Due to duplicates   : " + str(analysis_results['dupe']))
    print("    Due to wrong length : " + str(analysis_results['wrg']))
    print("    Due to neg. numbers : " + str(analysis_results['neg']))

    # print(nrows_parsed)

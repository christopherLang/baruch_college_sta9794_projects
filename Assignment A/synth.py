import os
import numpy as np
from datetime import datetime as dtdt
import datetime as dt
import json
import sys


def increment_time(start_datetime, n=10000):
    time_beta = np.float64(1) / np.float64(0.9000992)  # in milliseconds (ms)
    time_diff = np.random.exponential(time_beta, n) * 1000
    time_diff = np.cumsum(time_diff)
    time_diff = [dt.timedelta(microseconds=i) for i in time_diff.tolist()]

    result = [(start_datetime + i) for i in time_diff]

    return result


def get_prices(n=10000):
    random_prices = np.random.lognormal(7.1528596, 0.1721931, n)

    return random_prices.tolist()


def get_units(n=10000):
    random_units = np.random.uniform(1, 749998, n)

    return random_units.tolist()


if __name__ == "__main__":
    wdir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(wdir)

    with open("config/synth_config.json", "r") as file:
        cfg = json.load(file)

    file = open(cfg['saveloc'], "w")
    file.close()

    current_time = dtdt.utcnow()
    current_size = 0
    print_index = 0
    while current_size < cfg['total_size_gb']:
        data_dt = increment_time(current_time, cfg['chunk'])
        current_time = data_dt[-1]
        data_p = get_prices(cfg['chunk'])
        data_u = get_units(cfg['chunk'])

        synth_data = list()
        for d_datetime, d_prices, d_units in zip(data_dt, data_p, data_u):
            probs = np.random.sample(3) < cfg['noise_probs']
            row = list()
            if bool(probs[0]) is True:
                row.append(d_datetime.strftime("%Y%m%d:%H:%M:%S.%f") + "A")
            else:
                row.append(d_datetime.strftime("%Y%m%d:%H:%M:%S.%f"))

            if bool(probs[1]) is True:
                price_noise_types = np.random.sample(2)
                if price_noise_types[0] < 0.80:
                    d_prices *= -1

                if price_noise_types[1] < 0.30:
                    d_prices *= np.random.uniform(10000, 100000)

            row.append(str(d_prices))

            if bool(probs[2]) is True:
                unit_noise_types = np.random.sample(1)
                if unit_noise_types[0] < 0.50:
                    d_units = 0

                else:
                    d_units = np.random.uniform(0, 300)

            row.append(str(d_units))

            synth_data.append(",".join(row))

        with open(cfg['saveloc'], "a") as file:
            file.writelines([i + "\n" for i in synth_data])

        current_size = os.stat(cfg['saveloc']).st_size  # in bytes
        current_size *= 1e-9  # convert to GB, roughly

        print_index += 1

        if print_index % 2 == 0:
            print_msg = "Current size: {0}GB \r".format(round(current_size, 4))
            sys.stdout.write(print_msg)
            sys.stdout.flush()
            print_index = 0

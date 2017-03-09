import numpy as np


def moments(collection, n=1):
    x = np.array(collection)

    the_mean = np.mean(x)
    the_stdev = np.std(x)

    result = np.sum(np.power(x - the_mean, n)) / np.float64(len(x))
    result /= np.power(the_stdev, n)

    return result


def kurtosis(collection, excess=True):
    result = moments(collection, 4)

    if excess:
        result = result - np.float64(3)

    return result


def skewness(collection):
    result = moments(collection, 3)

    return result


def jarque_bera(collection):
    smpl_kurt = kurtosis(collection, True)
    smpl_skew = skewness(collection)
    sample_size = np.float64(len(collection))

    test_stat = sample_size / np.float64(6)
    test_stat *= (np.power(smpl_skew, 2) +
                  np.power(smpl_kurt, 2) / np.float64(4))

    result = {
        "test_stat": test_stat,
        "a95": test_stat <= np.float64(5.99),
        "a99": test_stat <= np.float64(9.21),
        "a999": test_stat <= np.float64(13.8)
    }

    return result

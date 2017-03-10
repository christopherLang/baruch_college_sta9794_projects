from pyspark import SparkContext
import numpy as np

# We will use the old standard circle and square method
# The approximation is done using the following formula:
#  pi ~= 4 * M / N
# where M is number of points landing in the inscribed circle
# and N is the number of points generated, in this case iterations

# This program was tested on a Microsoft Azure HDInsight Spark instance, with
# 4 nodes and 24 cores

# program setup
iteration = 1000000000  # 1 billion random points
partition = 25
radius = 1


def pi_estimation(data_packet):
    n = data_packet['n']
    radius = data_packet['radius']
    radius_sq = np.int64(pow(radius, 2))
    # We utilize numpy arrays for vectorized work
    # First, center both the square and circle on the origin in cartesian
    # coordinates
    # Second, generate n (x, y) points, bounded in the square
    xpos = np.random.uniform(-1 * radius, radius, n)
    ypos = np.random.uniform(-1 * radius, radius, n)

    # Third, since the square/circle is centered on origin, we convert the n
    # (x, y) points into polar coordinates, mainly the radius from origin
    # If the radii found is beyond the set radius, they're assumed to have
    # landed in the square. Otherwise, in the inscribed circle
    #
    # To get radii, we use pythagoras theorem to find hypotenus
    in_square = (np.power(xpos, 2) + np.power(ypos, 2)) > radius_sq

    n_square = np.sum(in_square, dtype=np.int64)
    n_circle = np.int64(in_square.size) - n_square

    result = {
        "in_circle": n_circle,
        "in_square": n_square,
        "nruns": n
    }

    return result


def reduce_estimate(objA, objB):
    result = {
        "in_circle": objA['in_circle'] + objB['in_circle'],
        "in_square": objA['in_square'] + objB['in_square'],
        "nruns": objA['nruns'] + objB['nruns']
    }

    return result


sc = SparkContext("local", "Pi Circle/Square Approximation")

worker_packet = list()
for _ in range(0, partition):
    worker_packet.append({
        "n": np.int64(iteration / partition),
        "radius": np.int64(radius)
    })

distIn = sc.parallelize(worker_packet, partition)

result = distIn.map(pi_estimation).reduce(reduce_estimate)

pi = np.float64(result['in_circle']) / np.float64(result['nruns'])
pi *= np.float64(4)

nruns = result['nruns']

print("Pi estimation is {0} for {1} simulation points".format(pi, nruns))
print("N points in circle: {0}".format(result['in_circle']))
print("N points in square: {0}".format(result['in_square']))

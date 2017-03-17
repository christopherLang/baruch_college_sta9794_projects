import math


def pretty_time_string(days=None, seconds=None, microseconds=None,
                       ndig_mins=2, ndig_secs=2):
    """Generate a pretty string, indicating time

    Generates a string, show elapsed time by:
      1. Number of days
      2. Number of hours
      3. Number of minutes
      4. Number of seconds

    Will only show if applicable (30 seconds has no days, or even minutes)
    """
    result = list()

    if days is not None:
        if days > 0:
            result.append(str(days) + " day(s)")

    total_seconds = seconds

    if microseconds is not None:
        total_seconds += (microseconds * 1e-6)

    if total_seconds >= 3600:
        total_hours = 0

        while total_seconds >= 3600:
            total_seconds -= 3600
            total_hours += 1

        result.append(str(total_hours) + " hour(s)")

    if total_seconds >= 60:
        total_minutes = 0

        while total_seconds >= 60:
            total_seconds -= 60
            total_minutes += 1

        total_minutes = round(total_minutes, ndig_mins)
        result.append(str(total_minutes) + " minute(s)")

    total_seconds = round(total_seconds, ndig_secs)
    result.append(str(total_seconds) + " second(s)")

    return ", ".join(result)


def execution_time(start_time, end_time):
    """Parse execution time in UTC

    Given two datetime objects, compute elapsed time in seconds and pretty
    string, generate formatted dates for both objects, and return original
    datetime objects

    Note: This function assumes that both start_time and end_time is UTC time.
          It will automatically apply a UTC time to the objects. If the objects
          are not UTC time then this application would have incorrect time

    Args:
        start_time, end_time (datetime):
            A datetime object

    Returns (dict):
    The dictionary has the following keys:
      1. pretty_str - a pretty string showing elapsed time
      2. seconds - Number of seconds elapsed between start_time and end_time
      3. start - formatted start date and time
      4. start_ios - ISO 8601 formatted start date and time
      5. end - formatted end date and time
      6. end_iso - ISO 8601 formatted start date and time
      7. raw - An array, containing start_time, end_time, and timedelta
    """
    result = dict()

    elapsed = end_time - start_time

    pretty_time = pretty_time_string(elapsed.days, elapsed.seconds,
                                     elapsed.microseconds)

    # if tz is None:
    #     tz = time.strftime("%z", time.gmtime())

    result['pretty_str'] = pretty_time

    result['start'] = start_time.strftime('%A %B %d %Y | %I:%M:%S%p UTC')
    result['start_iso'] = start_time.isoformat() + "Z"
    result['end'] = end_time.strftime('%A %B %d %Y | %I:%M:%S%p UTC')
    result['end_iso'] = end_time.isoformat() + "Z"

    result['seconds'] = elapsed.seconds + (elapsed.microseconds * 1e-6)

    result['raw'] = [start_time, end_time, elapsed]

    return result


def even_split(iterable, ngroups):
    n = int(math.floor(len(iterable) / ngroups))

    result = list()
    for i in range(0, len(iterable), n):
        result.append(iterable[i:(i + n)])

    return result


def size_sequencer(size, n_groups, start_index=0):
    assert n_groups > 0

    result = list()

    if n_groups >= 2:
        if size % n_groups == 0:
            subsize = int(size / n_groups)
            left_over = 0

        else:
            subsize = int(math.floor(size / n_groups))
            left_over = int(size - subsize * n_groups)

        i = start_index
        for _ in range(n_groups):
            r = (i, i + subsize - 1)

            result.append(r)

            i += subsize

        if left_over is not 0:
            result[-1] = (result[-1][0], result[-1][1] + left_over)

    else:
        # n_groups is == 1
        result.append((start_index, start_index + size - 1))

    return result


def gather_dict(listofdict):
    result = listofdict[0]

    for i in range(1, len(listofdict)):
        for key in listofdict[i]:
            result[key] += listofdict[i][key]

    return result

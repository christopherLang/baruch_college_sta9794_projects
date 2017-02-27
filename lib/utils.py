import time
import datetime as dt


def pretty_time_string(days=None, seconds=None, microseconds=None):
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

    total_seconds = seconds + (microseconds * 1e-6)

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

        result.append(str(total_minutes) + " minute(s)")

    result.append(str(round(total_seconds, 2)) + " second(s)")

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

    start_time = start_time.replace(tzinfo=dt.timezone.utc).astimezone(tz=None)
    end_time = end_time.replace(tzinfo=dt.timezone.utc).astimezone(tz=None)

    # if tz is None:
    #     tz = time.strftime("%z", time.gmtime())

    result['pretty_str'] = pretty_time

    result['start'] = start_time.strftime('%A %B %d %Y | %I:%M:%S%p %Z')
    result['start_iso'] = start_time.isoformat()
    result['end'] = end_time.strftime('%A %B %d %Y | %I:%M:%S%p %Z')
    result['end_iso'] = end_time.isoformat()

    result['seconds'] = elapsed.seconds + (elapsed.microseconds * 1e-6)

    result['raw'] = [start_time, end_time, elapsed]

    return result

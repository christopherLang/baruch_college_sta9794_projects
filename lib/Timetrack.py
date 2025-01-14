import datetime as dt
from utils import pretty_time_string, execution_time


class Timetrack(object):
    """ Easily track time for logging and code performance

    Creating an object of this class will immediately begin tracking time since
    object construction. All date and time are stored as UTC time

    Multiple instances of time can be tracked by specifying the tag parameter
    that most methods have. The internal instance is 'root', but any name
    can be provided as well

    Some important methods would be:
      - now() : Returns the current UTC time. If time was paused, returns the
                the time when pausing occurred

      - elapsed() : Returns elapsed time since start, or when the object was
                    paused. The object returned are Python's timedelta objects

      - elapsed_seconds() : Similar to elapsed(), the difference being that the
                            number of seconds are returned instead of timedelta

      - elapsed_pretty() : Similar to elapsed(), but returns a string. The
                           string states the number of days, hours, minutes,
                           and seconds that has elapsed, depending on the how
                           long it actually has been
    """

    def __init__(self, initialize_time=True):
        self.int_time = dict()
        self.int_time['root'] = dict()
        self.int_time['root']['start'] = None
        self.int_time['root']['end'] = None

        if initialize_time is True:
            self.int_time['root']['start'] = self.now()

    def now(self, tag='root'):
        """Get the current date and time, in UTC, as Python datetime object

        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time

        Return:
          A datetime Python object, specifying the date and time in UTC
        """
        if self.int_time[tag]['end'] is None:
            return dt.datetime.utcnow()

        else:
            return self.int_time[tag]['end']

    def get_instances(self):
        """Get all instance names stored in this object

        Return:
          A list of strings, specifying instance names currently tracked
        """
        return list(self.int_time.keys())

    def reset_time(self, tag='root'):
        """Set the start time to current date and time

        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time

        Return:
          Nothing is returned
        """
        self.int_time[tag]['start'] = self.now(tag=tag)

    def new_time(self, tag):
        """Create new time instance named tag and start time

        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time

        Return:
          Nothing is returned
        """

        self.int_time[tag] = dict()
        self.int_time[tag]['start'] = dt.datetime.utcnow()
        self.int_time[tag]['end'] = None

    def pause_time(self, tag='root'):
        """Pause tracking of time

        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time

        Return:
          Nothing is returned
        """
        self.int_time[tag]['end'] = self.now(tag=tag)

    def unpause_time(self, tag='root'):
        """Set end time to None, effectively resetting pause time

        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time

        Return:
          Nothing is returned
        """
        self.int_time[tag]['end'] = None

    def set_time(self, date_time, tag='root'):
        """Sets start time to provided datetime object

        Please note that the datetime object provided MUST contain UTC time.
        Otherwise, all other returned times (e.g. elapsed) will be wrong

        Parameter:
          date_time : datetime.datetime
            A Python datetime object in UTC time

          tag : str
            Name of time instance. Default is 'root', the internal base time

        Return:
          Nothing is returned
        """
        self.int_time[tag]['start'] = date_time

    def get_start_time(self, tag='root'):
        """Get the stored start time

        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time

        Return:
          A datetime.datetime object, specifying the start time of instance
        """
        return self.int_time[tag]['start']

    def elapsed(self, tag='root'):
        """Get timedelta object, indicating elapse time since start

        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time

        Return:
          A datetime.timedelta object, specifying the instance's elapsed time
        """
        elapsed = self.now(tag=tag) - self.int_time[tag]['start']

        return elapsed

    def elapsed_seconds(self, tag='root'):
        """Get the number of seconds since start, as a float

        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time

        Return:
          A float, specifying the instance's elapsed time, in seconds
        """
        elapsed = self.elapsed(tag=tag)
        elapsed = elapsed.seconds + (elapsed.microseconds * 1e-6)

        return elapsed

    def elapsed_pretty(self, tag='root', **kwargs):
        """Get elapsed time as a string

        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time

        Return:
          A string, written in day/hour/minute/second, depending on length
        """
        elapsed = self.elapsed(tag=tag)
        elapsed_str = pretty_time_string(elapsed.days, elapsed.seconds,
                                         elapsed.microseconds, **kwargs)

        return elapsed_str

    def execution(self, tag='root'):
        """Get a dict with several time statistics

        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time

        Return:
          A dict, specifying the instance's time statistics
        """
        exec_time = execution_time(self.int_time[tag]['start'],
                                   self.now(tag=tag))

        return exec_time

    def start_time_pretty(self, tag='root'):
        """Get the start time, as a pretty formatted string

        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time

        Return:
          A string, specifying the instance's start time in pretty format
        """
        start_time = self.execution(tag=tag)['start']

        return start_time

    def end_time_pretty(self, tag='root'):
        """Get the end time, as a pretty formatted string

        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time

        Return:
          A string, specifying the instance's end time in pretty format
        """
        end_time = self.execution(tag=tag)['end']

        return end_time

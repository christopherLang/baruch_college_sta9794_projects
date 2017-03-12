import datetime as dt
from utils import pretty_time_string, execution_time


class Timetrack(object):
    def __init__(self, initialize_time=True):
        self.start_time = None
        self.end_time = None

        if initialize_time is True:
            self.start_time = self.now()

    def now(self):
        if self.end_time is None:
            return dt.datetime.utcnow()

        else:
            return self.end_time

    def reset_time(self):
        self.start_time = self.now()

    def pause_time(self):
        self.end_time = self.now()

    def start_time(self):
        self.end_time = None

    def set_time(self, date_time):
        self.start_time = date_time

    def get_start_time(self):
        return self.start_time

    def elapsed(self):
        elapsed = self.now() - self.start_time

        return elapsed

    def elapsed_seconds(self):
        elapsed = self.elapsed()
        elapsed = elapsed.seconds + (elapsed.microseconds * 1e-6)

        return elapsed

    def elapsed_pretty(self):
        elapsed = self.elapsed()
        elapsed_str = pretty_time_string(elapsed.days, elapsed.seconds,
                                         elapsed.microseconds)

        return elapsed_str

    def execution(self):
        exec_time = execution_time(self.start_time, self.now())

        return exec_time

    def start_time_pretty(self):
        start_time = self.execution()['start']

        return start_time

    def end_time_pretty(self):
        end_time = self.execution()['end']

        return end_time

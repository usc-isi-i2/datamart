from datamart.profilers.basic_profiler import BasicProfiler
from datamart.profilers.dsbox_profiler import DSboxProfiler


class Profiler(object):

    def __init__(self):
        self.basic_profiler = BasicProfiler()
        self.dsbox_profiler = DSboxProfiler()

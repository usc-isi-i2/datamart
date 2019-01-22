from datamart.profilers.basic_profiler import BasicProfiler
from datamart.profilers.dsbox_profiler import DSboxProfiler
from datamart.profilers.two_raven_profiler import TwoRavenProfiler


class Profiler(object):

    def __init__(self):
        self.basic_profiler = BasicProfiler()
        self.dsbox_profiler = DSboxProfiler()
        self.two_raven_profiler = TwoRavenProfiler()

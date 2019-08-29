from EventFile import EventFile
import numpy
import uproot

import json
import sys

class UpRootEventFile(EventFile):
    def __init__(self, root_file, repeat = 1):
        self.uproot_f = uproot.open(root_file)
        self.repeat   = repeat

        super(UpRootEventFile, self).__init__(root_file)

    @property
    def maxend(self):
        return len(self.uproot_f['Events']) * self.repeat

    def events_at(self, columns, start=0, count=None):
        end=None
        if count:
            end = start + count
        all_events = self.uproot_f['Events'].arrays(columns, entrystart=start, entrystop=end, outputtype=tuple)

        for n in range(self.repeat):
            for i in range(len(all_events[0])):
                row = [ col[i] for col in all_events ]
                yield [ self._topynums(x) for x in row ]

    def _topynums(self, x):
        if isinstance(x, (numpy.float32, numpy.float64)):
            return float(x)
        elif isinstance(x, numpy.ndarray):
            return x.tolist()
        else:
            return x

    def all_columns(self):
        return self.uproot_f['Events'].keys()

    def __hash__(self):
        return hash('UpRootEventFile', self.root_file)


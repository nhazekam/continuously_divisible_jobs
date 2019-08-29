import EventFile
from ROOT import TFile

import json
import sys

class PyRootEventFile(EventFile):
    def __init__(self, root_file, columns, start=None, count=None):
        self.pyroot_f = TFile(root_file)
        super(PyRootEventFile, self).__init__(root_file, columns, start, count)

    @property
    def tree(self):
        return self.pyroot_f.events

    @property
    def tree_len(self):
        return self.tree.GetEntriesFast()

    def event_at(self, idx):
        self.tree.GetEntry(self.start + idx)
        values = {}
        for c in self.columns:
            val = getattr(self.tree, c)
            try:
                values[c] = list(val)
            except:
                values[c] = val
        return values

    def events_at(self, start=0, count=None):
        end = None
        if count:
            end = start + count



import json
import sys

class EventFile(object):
    def __init__(self, root_file):
        self.root_file = root_file

    def events_at(self, columns, start=0, count=None):
        raise NotImplemented

    @property
    def maxend(self):
        raise NotImplemented

    def __contains__(self, idx):
        return 0 <= idx and idx < self.maxend

    def __len__(self):
        return self.count

    def __str__(self):
        return '{}-{}'.format(0, self.maxend)

    def to_json(self, output_name):
        with open(output_name, 'w') as output_f:
            for event in self:
                output_f.write(json.dumps(event))
                output_f.write('\n')


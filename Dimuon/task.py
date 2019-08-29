#! /usr/bin/env python

import importlib
import dimuon
import json
from SQLEventFile import SQLEventFile
from UpRootEventFile import UpRootEventFile

from sys import argv, exit

event_type, event_file, output_file, loffset, lrange, repeat = argv[1:7]
columns = argv[7:]

loffset = int(loffset)
lrange  = int(lrange)
repeat  = int(repeat)

event_class = getattr(importlib.import_module(event_type), event_type)

events = event_class(event_file, repeat = repeat)
print(events)

print(loffset, lrange)
with open(output_file, 'w') as out_f:
    for event in events.events_at(columns, loffset, lrange):
        result = dimuon.dimuonCandidate(*event)
        if result['pass']:
            out_f.write(json.dumps(result))
            out_f.write('\n')


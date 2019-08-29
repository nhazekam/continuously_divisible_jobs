import sqlite3
import csv

import math
import os
import sys
import re
import time

import numpy
from UpRootEventFile import UpRootEventFile


def db_init(cur, particle, columns):
    #create particle table
    specs = []
    for field in columns:
        if field == 'event' or field == db_count_name(particle) or re.search('.*Id$', field):
            specs.append('{} INTEGER'.format(field))
        else:
            specs.append('{} FLOAT'.format(field))
    cur.execute('CREATE TABLE {} ({})'.format(particle, ','.join(specs)))

def root_get_columns(event, columns):
    values = []
    for i in range(len(columns)):
        val = event[i]
        try:
            values.append(list(val))
        except:
            values.append(val)
    return values

def db_num_or_null(val):
    if isinstance(val, bool):
        return str(int(val))
    elif isinstance(val, (int, float, numpy.uint64)) and not math.isnan(val):
        return str(val)
    else:
        return 'NULL'

def db_insert_row(cur, particle, columns, row):
    #print(row)
    #print(columns)
    #print('INSERT INTO {} ({}) VALUES ({})'.format(particle, ','.join(columns), ','.join(row)))
    try:
        cur.execute('INSERT INTO {} ({}) VALUES ({})'.format(particle, ','.join(columns), ','.join(row)))
    except:
        print(row)
        print(columns)
        print(len(row), len(columns))
        print('INSERT INTO {} ({}) VALUES ({})'.format(particle, ','.join(columns), ','.join(row)))
        raise

def db_to_csv(cur, particle, columns, filename):
    with open(filename, 'w') as fo:
        csvw = csv.writer(fo)
        csvw.writerow(columns)
        csvw.writerows(cur.execute('SELECT {} FROM {}'.format(','.join(columns), particle)))

def db_count_events(cur, particle):
    cur.execute('SELECT COUNT(DISTINCT event) FROM {}'.format(particle))
    return cur.fetchone()[0]

def db_get_columns_names(root_file):
    return [str(x) for x in root_file['Events'].keys()]

def db_particle_columns(all_columns):
    return [re.sub('^n', '', x) for x in all_columns if re.match('n', x)]

def db_count_name(particle_name):
    return 'n' + particle_name

def db_columns_of(all_columns, particle):
    return ['event', db_count_name(particle)] + [x for x in all_columns if re.match(particle,x)]

def db_value(lst, i):
    if isinstance(lst, list):
        try:
            return db_num_or_null(lst[i])
        except IndexError:
            try:
                return db_num_or_null(lst[0])
            except IndexError:
                return db_num_or_null(None)
    else:
        return db_num_or_null(lst)

def db_tabular_of_particle(event, event_id, count, columns):
    tabular = []
    for i in range(count):
        tabular.append([str(event_id), str(count)] + [db_value(x, i) for x in event[2:]])
    return tabular

def db_insert_events(cur, particle, events, columns):
    #n = 10000
    for event in events:
        db_insert_event(cur, particle, event, columns)

def db_insert_event(cur, particle, event, columns):
    global o
    event_id = event[0]
    count    = event[1]

    tabular_form = db_tabular_of_particle(event, event_id, count, columns)
    for row in tabular_form:
        db_insert_row(cur, particle, columns, row)

input_file  = sys.argv[1]
output_file = sys.argv[2]

if os.path.exists(output_file):
    os.unlink(output_file)

efile = UpRootEventFile(input_file)

particle         = 'Muon'
all_columns      = efile.all_columns()
particle_columns = db_columns_of(all_columns, particle)
events           = efile.events_at(particle_columns)

db   = sqlite3.connect(output_file)
cur  = db.cursor()
db_init(cur, particle, particle_columns)


print('Reading data')
db_insert_events(cur, particle, events, particle_columns)
db.commit()

print('Processing data')
print('Number of events: {}'.format(db_count_events(cur, particle)))
print(cur.execute('SELECT COUNT(event) FROM {}'.format(particle)).fetchone()[0])

print('Writing output data')
db_to_csv(cur, particle, particle_columns, 'output.csv')


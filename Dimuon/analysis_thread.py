import sys
import os
import json

from DivisibleJob import Job, TaskResult
from multithread_job_driver import run_jobs

from DimuonTask import DimuonTask
from UpRootEventFile import UpRootEventFile
from SQLEventFile import SQLEventFile

try:
    kind = sys.argv[1]
except:
    raise Exception('{} root|sql [partition] [repeat]'.format(sys.argv[0]))

try:
    size = int(sys.argv[2])
except:
    size = 50000

try:
    repeat = int(sys.argv[3])
except:
    repeat = 1

try:
    dyn = sys.argv[4]
except:
    dyn = "ft"


data_file, event_file = None, None

if kind == 'root':
    data_file  = 'DYJetsToLL_M.root'
    event_file = UpRootEventFile(data_file, repeat)
elif kind == 'sql':
    data_file  = 'DYJetsToLL_M.db'
    event_file = SQLEventFile(data_file, repeat)
else:
    raise Exception('{} root|sql'.format(sys.argv[0]))

input_files = ["dimuon.py", "EventFile.py", "DimounRootTask.py", "DivisibleJob.py", "SQLEventFile", "UpRootEventFile", data_file] 
output_file = 'output.json'

dimdetect  = DimuonTask(event_file, output_file, input_files, environment = {}, repeat = repeat)

job = Job(specs = [dimdetect])
time  = 30000 # Time in seconds to run a batch of jobs
# size  = 50000 # Number of slices per jobs
count = len(dimdetect) / size # number of jobs with size 

job = Job(specs = [dimdetect])

if dyn == "ft":
    (jobs, sys_time, avail_time, split_time, join_time) = run_jobs(job, size, count, time)
else:
    (jobs, sys_time, avail_time, split_time, join_time) = run_jobs(job, size, count, time, False, True)

print "Size of jobs vs initial : {} : {}".format(len(jobs), len(job))

# create histogram
#import dimuon
#for s in jobs.specs:
#    dimuon.histogram_fill_from_file(s.output_file)
#dimuon.histogram(output = 'output.png')


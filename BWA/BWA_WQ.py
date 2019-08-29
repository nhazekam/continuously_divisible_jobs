
import sys
import pickle
import os
import argparse
from DivisibleJob import Job, TaskResult
from BWAQuery import *
from VirtualFile import *
import time

from work_queue import *
from wq_job_driver import *

parser = argparse.ArgumentParser()
parser.add_argument("ref")
parser.add_argument("input")
parser.add_argument("output")
parser.add_argument("size")
parser.add_argument("test")
parser.add_argument("type")
parser.add_argument("first", type=int)
parser.add_argument("second", type=int, nargs='?', default=1)
parser.add_argument("time", type=int, nargs='?', default=60)
parser.add_argument("lock", type=str, nargs='?', default='n')
parser.add_argument("core", type=int, nargs='?', default='1')
parser = add_parse_arguments(parser)

args = parser.parse_args()

print "Passed configuration {} {} {} {} {} {} {} {}".format(args.ref, args.input, args.output, args.size, args.test, args.type, args.first, args.second)

reference=["{}{}".format(args.ref, i) for i in ["", ".amb", ".ann", ".bwt", ".pac", ".sa"]]
input_files=["bwa", "VirtualFile.py", "DivisibleJob.py", "BWAQuery.py"]+reference

bwa_seq = None
completed_size = 0

if args.type == "simple":
    bwa_seq = BWAQuerySimple(args.input, args.output, input_files, [], environment = { 'reference': args.ref })
elif args.type == "simple-byte":
    bwa_seq = BWAQuerySimpleByte(args.input, args.output, input_files, [], environment = { 'reference': args.ref })
elif args.type == "logical-vf":
    bwa_seq = BWAQueryLogicalVF(args.input, args.output, input_files, [],  environment = { 'reference': args.ref })
elif args.type == "index-vf":
    data_file = FastqFile.from_file(args.input, True)
    print "Data file being used : {}".format(data_file.data_name)
    bwa_seq = BWAQueryIndexVF(data_file, os.path.basename(args.output), input_files, [], environment = { 'reference': args.ref })


print "Starting configuration {} {} {} {} {} {} {} {}".format(args.ref, args.input, args.output, args.size, args.test, args.type, args.first, args.second)

job = Job(specs = [bwa_seq])

completed_size = run_job(job, os.path.basename(args.output), args.first, args.second, args.time, args.lock, args)

os.rename(os.path.basename(args.output), args.output)

print "{} {}".format(len(bwa_seq), completed_size)
result = len(bwa_seq) - completed_size
sys.exit(result)



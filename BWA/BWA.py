import sys
import pickle
import os
import argparse
from DivisibleJob import Job, TaskResult
from BWAQuery import *
from VirtualFile import *

from local_job_driver import *

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
args = parser.parse_args()


reference=["{}{}".format(args.ref, i) for i in ["", ".amb", ".ann", ".bwt", ".pac", ".sa"]]
input_files=["bwa", "bwa_runner", "DivisibleJob.py", "BWAQuerySimple.py"]+reference

bwa_seq = None

if args.type == "simple":
    bwa_seq = BWAQuerySimple(args.input, args.output, input_files, [], environment = { 'reference': args.ref })
elif args.type == "simple-byte":
    bwa_seq = BWAQuerySimpleByte(args.input, args.output, input_files, [], environment = { 'reference': args.ref })
elif args.type == "logical-vf":
    bwa_seq = BWAQueryLogicalVF(args.input, args.output, input_files, [],  environment = { 'reference': args.ref })
elif args.type == "index-vf":
    data_file = FastqFile.from_file(args.input)
    bwa_seq = BWAQueryIndexVF(data_file, args.output, input_files, [], environment = { 'reference': args.ref })

print len(bwa_seq)
completed_size = 0

job = Job(specs = [bwa_seq])

if args.test == "ft":
    completed_size = flat_run(job, args.output, args.first)
elif args.test == "tr":
    completed_size = tiered_run(job, args.output, args.first, args.second)
elif args.test == "tl":
    completed_size = time_limit_run(job, args.output, args.first, args.second)
elif args.test == "tla":
    completed_size = time_limit_adjust_run(job, args.output, args.first, args.second, args.time, args.lock)
elif args.test == "dyn":
    completed_size = dyn_run(job, args.output, args.first)

print "{} {}".format(len(bwa_seq), completed_size)
result = len(bwa_seq) - completed_size
sys.exit(result)



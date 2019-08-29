
import os
import tarfile
import time
import argparse

import sys
import pickle
import json
from DivisibleJob import Job, TaskResult
from DivisibleJobStats import *
from VirtualFile import *

from work_queue import *

category_name = "default"

#---Process options on the command line
def add_parse_arguments(parser):
    parser.add_argument('--port', type=int, help="Sets the port for work_queue to listen on.", default=WORK_QUEUE_DEFAULT_PORT)
    parser.add_argument('--fa', dest="fast_abort", type=int, help="Sets the work_queue fast abort option with the given multiplier (default: off).")
    parser.add_argument('-N', dest="project", help="Sets the project name to <project> (default: none).")
    parser.add_argument('--stats', type=str, help="Prints WQ statistics to <file> (default:off).")
    parser.add_argument('--trans', type=str, help="Prints WQ statistics to <file> (default:off).")
    parser.add_argument('-d', dest="debug", type=str, help="Sets the debug flag for Work Queue. For all debugging output, try 'all' (default: off).")
    parser.add_argument('--debug-log', type=str, help="Sets the debug output location, defualt is STDOUT")
    parser.add_argument('--monitor', action='store_true', help="Specify monitor directory.")
    parser.add_argument('--monitor-dir', type=str, help="Specify monitor directory.", default="resource_monitor")
    parser.add_argument('--cores', type=int, help="Specify task cores needs.", default=1)
    parser.add_argument('--memory', type=int, help="Specify task memory needs in MB.", default=2048)
    parser.add_argument('--disk', type=int, help="Specify task disk needs in MB.", default=2048)
    parser.add_argument('--mode', type=str, help="Specify exec mode (multicore, seq).", default="multicore")

    return parser


#------------------------------- FUNCTION DEFINITIONS-------------------------------------
def setup_workqueue (args):
    if (args.debug):
        cctools_debug_flags_set(args.debug)
        if (args.debug_log):
            cctools_debug_config_file(args.debug_log)
        print "{} Work Queue debug flags set: {}.\n".format(time.asctime(), args.debug)

    try:
        wq = WorkQueue(args.port)
    except:
        print "Instantiation of Work Queue failed!"
        sys.exit(1)

    print "{} Work Queue listening on port {}.\n".format(time.asctime(), wq.port)

    if(args.fast_abort) :
        wq.activate_fast_abort_category(category_name, args.fast_abort)
        print "{} Work Queue fast abort set to $multiplier.\n".format(time.asctime())

    if(args.monitor) :
        wq.enable_monitoring(args.monitor_dir)
        print "{} Work Queue enabled monitoring at {}.\n".format(time.asctime(), args.monitor_dir)

    if(args.project) :
        wq.specify_name(args.project)
        print "{} Work Queue project name set to {}\n".format(time.asctime(), args.project)

    if(args.stats) :
        wq.specify_log(args.stats)
        print "{} Work Queue stats file set to {}\n".format(time.asctime(), args.stats)

    if(args.trans) :
        wq.specify_transactions_log(args.trans)
        print "{} Work Queue trans file set to {}\n".format(time.asctime(), args.trans)

    resources = {}
    if(args.memory) :
        resources['memory'] = args.memory

    if(args.core) :
        resources['cores'] = args.core
    elif(args.cores) :
        resources['cores'] = args.cores

    if(args.disk) :
        resources['disk'] = args.disk

    wq.specify_category_max_resources(category_name, resources)

    return wq

# Create and submit job
def submit_job (wq, job, size, count, timeout, cores):
    desc_name = "task_input_desc_{}.json".format(job)
    desc_oname = "task_output_desc_{}.json".format(job)
    desc_otar = "task_output_desc_{}.tar".format(job)

    job_desc = job.to_description()
    with open(desc_name, 'w') as desc_file:
        json.dump(job_desc, desc_file)

    command = "python ./multicore_job_driver.py {} {} {} {} {} {} --cores {}".format(
                        desc_name, desc_oname, desc_otar, size, count, timeout, cores)
    t = Task(command)
    t.specify_input_file(desc_name)

    input_set = {"multicore_job_driver.py", "DivisibleJob.py", "DivisibleJobStats.py"}
    output_set = {desc_oname, desc_otar}
    input_set.update(job.input_files)

    for file in input_set:
        if isinstance(file, VirtualFile) or issubclass(file.__class__, VirtualFile):
            # This references a piece of the data
            if file.parent:
                print "file_piece {} {} {} {}".format(file.parent.data_name, file.data_name, file.start_byte, file.end_byte -1)
                t.specify_file_piece(file.parent.data_name, file.data_name, file.start_byte, abs(file.end_byte -1), WORK_QUEUE_INPUT)
            # There is no parent, so it is just the full data set
            else:
                t.specify_input_file(file.data_name, cache=False)
        else:
            t.specify_input_file(file, cache=True)

    for file in output_set:
        t.specify_output_file(file)

    t.specify_tag(str(job)) 

    t.specify_category(category_name)
    
    taskid = wq.submit(t)
    print "{} Submitted task (id# {}): {} : {}\n".format(time.asctime(), t.tag, t.command, t.result)
    return True


# Wait on tasks
def retrieve_task(wq):
    retrieved_tasks = 0

    print "{} Waiting on tasks to complete...\n".format(time.asctime())
    t = wq.wait(30)

    if t:
        print "{} Task (id# {}) complete: {} (return code {})\n".format(time.asctime(), t.tag, t.command, t.return_status)
        if(t.return_status != 0) or (t.result != WORK_QUEUE_RESULT_SUCCESS) :
            print "{} Task (id# {}) failed : Return Code {} : Result {}\n".format(time.asctime(), t.tag, t.return_status, t.result)
            print "{} .\n".format(t.output)

        desc_name = "task_input_desc_{}.json".format(t.tag)
        desc_oname = "task_output_desc_{}.json".format(t.tag)
        desc_otar = "task_output_desc_{}.tar".format(t.tag)
        with open(desc_oname, 'r') as desc_file:
            report = json.load(desc_file)
            job = Job.from_description(report["job"])

        job_otar = tarfile.open(desc_otar)
        job_otar.extractall()
        job_otar.close()

        os.unlink(desc_name)
        os.unlink(desc_oname)
        os.unlink(desc_otar)

        retrieved_tasks += 1
        print "{} Retrieved {} tasks.\n".format(time.asctime(), retrieved_tasks)
        print "{} .\n".format(t.output)
        return (job, report["sys_time"], report["comp_time"], report["split_time"], report["join_time"])
    else :
        print "{} Retrieved {} tasks.\n".format(time.asctime(), retrieved_tasks)
        return None

def wq_hungry(wq, num_tasks, retrieved_tasks, cores_per_job):
    ratio = 2
    if(((num_tasks-retrieved_tasks)*cores_per_job) < (ratio * max(wq.stats.total_cores, 1))): # SHOULD BE CORES IF CATEGORY IS SPECIFIED
        return True
    return False

def run_job(job, output, first_split, second_split, time_slice, fixed_size, args):
    wq = setup_workqueue(args)

    cores = args.cores
    if args.core:
        cores = args.core

    num_tasks=0
    retrieved_tasks=0

    fsize = int(first_split)
    ssize = int(second_split)
    total = len(job)
    count = total / fsize
    tp = 0

    sys = 0
    avail = 0
    split = 0
    join = 0

    time_limit = int(time_slice)

    completed = None
    size_completed = 0
    failed = None
    size_failed = 0

    jobs = job

    if fixed_size == 'f' or fixed_size == 'b':
        job_size_stats = JobStatFixed(total, fsize)
    else:
        job_size_stats = JobStatHillClimb(total, fsize)

    if fixed_size == 's' or fixed_size == 'b':
        worker_size_stats = JobStatFixed(ssize, ssize)
    else:
        worker_size_stats = JobStatHillClimb(ssize, ssize)
        
    while size_completed + size_failed < total:
        while wq_hungry(wq, num_tasks, retrieved_tasks, cores) and (jobs is not None) and (len(jobs) > 0):
            s_start = time.time()
            job_split = jobs.split(job_size_stats.get_next(), 1)
            s_end = time.time()
            split = s_end - s_start
            candidate = job_split[0]
            if len(job_split) > 1:
                jobs = job_split[1]
                print "Remaining Jobs {}".format(str(jobs))
            else:
                jobs = None
            s_size = worker_size_stats.get_next()
            s_count = max(len(candidate)/s_size, 1)
            if(submit_job(wq, candidate, s_size, s_count, time_limit, cores)):
                num_tasks += 1

        r_jobs = None
        if((num_tasks-retrieved_tasks) > 0):
            job_tuple = retrieve_task(wq)
            if job_tuple:
                retrieved_tasks += 1
                (r_jobs, sys_time, comp_time, split_time, join_time) = job_tuple
                sys += sys_time
                avail += comp_time
                split += split_time
                join += join_time
                (unrun, fail, complete) = r_jobs.categorize_jobs()

                u_size = len(unrun) if unrun else 0
                f_size = len(fail) if fail else 0 
                c_size = len(complete) if complete else 0

                effective_time = (sys_time - (split_time + join_time)+0.0)/max(sys_time, time_limit)
                effective_worker_tp = (c_size+0.0)/max(sys_time, time_limit)
                print "Effective Time {} {} {}".format(effective_worker_tp, sys_time, time_limit)
                job_size_stat = JobStat(len(r_jobs), effective_worker_tp)
                job_size_stats.update_model(job_size_stat)
        
                effective_tp = (c_size+0.0)/comp_time
                print "Effective TP {} {} {}".format(effective_tp, c_size, comp_time)
                worker_size_stat = JobStat(s_size, effective_tp)
                worker_size_stats.update_model(worker_size_stat)

                print("TIME: {} {} {} {}".format(sys, avail, split, join))

                print("Job States: {} {} {} {}".format(u_size, f_size, c_size,
                                                len(completed) if completed else 0))

                if unrun:
                    if jobs:
                        j_start = time.time()
                        jobs = jobs.join(unrun)
                        j_end = time.time()
                        join += j_end - j_start
                    else:
                        jobs = unrun

                if failed is None:
                    failed = fail
                else:
                    failed = failed.join(fail)
                size_failed = len(failed) if failed else 0

                if complete is not None:
                    if completed is None:
                        completed = complete
                    else:
                        j_start = time.time()
                        completed = completed.join(complete)
                        j_end = time.time()
                        join += j_end - j_start

                if completed is not None:
                    size_completed = len(completed)
        else:
            time.sleep(1)

    os.rename(completed.specs[0].output_file, output)
    return size_completed


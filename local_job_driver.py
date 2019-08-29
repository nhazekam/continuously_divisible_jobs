import sys
import pickle
import os
import argparse
from DivisibleJob import Job, TaskResult
from DivisibleJobStats import JobStat, JobStatHillClimb, JobStatFixed

from multicore_job_driver import *

def flat_run(job, output, split):

    time = 50000 # Time in seconds to run a batch of jobs
    size = int(split) # Number of slices per jobs
    count = len(job) / size # number of jobs with size 

    sys = 0
    avail = 0
    split = 0
    join = 0

    (jobs, sys_time, avail_time, split_time, join_time) = run_jobs(job, size, count, time)

    sys += sys_time
    avail += avail_time
    split += split_time
    join += join_time

    os.rename(jobs.specs[0].output_file, output)

    print "Time : {} {} {} {}".format(sys, avail, split, join)

    return len(jobs)


def dyn_run(job, output, split):

    time = 50000 # Time in seconds to run a batch of jobs
    size = int(split) # Number of slices per jobs
    count = len(job) / size # number of jobs with size 

    sys = 0
    avail = 0
    split = 0
    join = 0

    (jobs, sys_time, avail_time, split_time, join_time) = run_jobs(job, size, count, time, False, True)

    sys += sys_time
    avail += avail_time
    split += split_time
    join += join_time

    os.rename(jobs.specs[0].output_file, output)

    print "Time : {} {} {} {}".format(sys, avail, split, join)

    return len(jobs)


def tiered_run(job, output, first_split, second_split):
    time= 5000
    fsize = int(first_split)
    fcount = len(job) / fsize
    
    ssize = int(second_split)
    scount = fsize / ssize

    sys = 0
    avail = 0
    split = 0
    join = 0


    completed_jobs = None

    s_start = time.time()
    jobs = job.split(fsize, fcount)
    s_end = time.time()
    split += s_end - s_start

    for job in jobs:
        (cjob, sys_time, avail_time, split_time, join_time) = run_jobs(job, ssize, scount, time)

        sys += sys_time
        avail += avail_time
        split += split_time
        join += join_time

        if completed_jobs is None:
            completed_jobs = cjob
        else:
            completed_jobs = completed_jobs.join(cjob)

    os.rename(completed_jobs.specs[0].output_file, output)

    print "Time : {} {} {} {}".format(sys, avail, split, join)

    return len(completed_jobs)

def time_limit_run(job, output, first_split, time_slice):
    size = int(first_split)
    total = len(job)
    count = total / size
    
    completed = None
    size_completed = 0

    sys = 0
    avail = 0
    split = 0
    join = 0


    time_limit = int(time_slice)

    while size_completed < total:
        (cjob, sys_time, avail_time, split_time, join_time) = run_jobs(job, size, count, time_limit)

        sys += sys_time
        avail += avail_time
        split += split_time
        join += join_time
        (unrun, failed, complete) = jobs.categorize_jobs()

        print("Job States: {} {} {} {}".format(
                len(unrun) if unrun else 0, 
                len(failed) if failed else 0, 
                len(complete) if complete else 0, 
                len(completed) if completed else 0))
        if complete is None:
            print "No progress was made"
            return 0

        if completed is None:
            completed = complete
        else:
            j_start = time.time()
            completed = completed.join(complete)
            j_end = time.time()
            join += j_end - j_start
            size_completed = len(completed)

        job = unrun


    os.rename(completed.specs[0].output_file, output)

    print "Time : {} {} {} {}".format(sys, avail, split, join)

    return len(completed)

def time_limit_adjust_run(job, output, first_split, second_split, time_slice, fixed_size):
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
        (r_jobs, sys_time, comp_time, split_time, join_time) = run_jobs(candidate, s_size, s_count, time_limit)

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
        elif fail is not None:
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

    os.rename(completed.specs[0].output_file, output)

    print "Time : {} {} {} {}".format(sys, avail, split, join)

    return size_completed

def dynamic_run(job, output, first_split, second_split):
    time= 5000
    fsize = int(first_split)
    ssize = int(second_split)
    total = len(job)
    scount = fsize / ssize

    sys = 0
    avail = 0
    split = 0
    join = 0

    completed_jobs = None

    s_start = time.time()
    jobs = job.split(fsize, 1)
    s_end = time.time()
    split += s_end - s_start

    job = jobs[0]
    rest = jobs[1]
    while job:
        (cjob, sys_time, avail_time, split_time, join_time) = run_jobs(job, ssize, scount, time)

        sys += sys_time
        avail += avail_time
        split += split_time
        join += join_time
        if completed_jobs is None:
            completed_jobs = cjob
        else:
            completed_jobs = completed_jobs.join(cjob)
        if rest:
            s_start = time.time()
            jobs = rest.split(fsize, 1)
            s_end = time.time()
            split += s_end - s_start
            job = jobs[0]
            if len(jobs) > 1:
                rest = jobs[1]
            else:
                rest = None
        else:
            job = None

    os.rename(completed_jobs.specs[0].output_file, output)

    print "Time : {} {} {} {}".format(sys, avail, split, join)

    return len(completed_jobs)



import argparse
import json
from multiprocessing import Process, Queue, cpu_count
import pickle
import os
import sys
import tarfile
import time

from DivisibleJob import Job
from DivisibleJobStats import JobStat, JobStatHillClimb, JobStatFixed
from Queue import Empty

def run_job(src_q, dest_q):
    while True:
        job = src_q.get(True)

        if job is None:
            return
        job.execute()
        dest_q.put(job)
    return

def run_jobs_core_based(src_job, mult, time_limit):
    cores = cpu_count()
    count = mult * cores
    size  = max(int(len(src_job)/count), 1)
    return run_jobs(src_job, size, count, time_limit)

def run_jobs(src_job, size, count, time_limit, batch=True, dynamic=False, cores=None):
    start = time.time()
    print "DJR: Start {} Size {} Count {} Time {}".format(start, size, count, time_limit)

    if not cores:
        cores = cpu_count()
    total = len(src_job)
    split_time = 0
    join_time = 0

    if batch:
        s_start = time.time()
        on_cores = count-(count%cores)
        if on_cores < count:
            count = on_cores + cores
        size = max(int(total/count), 1)
        jobs = src_job.split(size, count)
        s_end = time.time()
        split_time += s_end - s_start
    else :
        jobs = src_job

    
    if dynamic:
        job_size_stats = JobStatHillClimb(total, size)
    else:
        job_size_stats = JobStatFixed(total, size)


    output_job = None

    limit = start + time_limit

    src_q = Queue()
    dest_q = Queue()
    procs = list()
    for i in range(0, cores):
        procs.append(Process(target=run_job, args=(src_q, dest_q,)))

    for p in procs:
        p.start()

    submitted = 0
    completed = 0
    running_hash = {}
    running_time = {}
    while ((jobs and len(jobs) > 0) or (submitted-completed) > 0) and time.time() < limit:
        while (submitted-completed) < cores and jobs and len(jobs) > 0:
            if batch:
                job = jobs.pop(0)
            else:
                s_start = time.time()
                size = job_size_stats.get_next()
                job_list = jobs.split(size, 1)
                s_end = time.time()
                split_time += s_end - s_start
                job = job_list[0]
                if len(job_list) > 1:
                    jobs = job_list[1]
                else:
                    jobs = None

            running_hash[str(job)] = job
            running_time[str(job)] = time.time()
            src_q.put(job)
            submitted += 1

        try:
            job = dest_q.get(True, timeout=1.0)
        except Empty:
            pass
        else:
            running_hash.pop(str(job), None)
            start_time = running_time.pop(str(job), time.time())
            completed += 1
            tp = ((len(job)+0.0)/(time.time()-start_time))
            job_size_stat = JobStat(len(job), tp)
            job_size_stats.update_model(job_size_stat)

            if output_job is None:
                output_job = job
            else:
                j_start = time.time()
                output_job = output_job.join(job)
                j_end = time.time()
                join_time += j_end - j_start

    for p in procs:
        src_q.put(None, False)
    
    for p in procs:
        if p.is_alive():
            p.terminate()

    for name, job in running_hash.items():
        if output_job is None:
            output_job = job
        else:
            j_start = time.time()
            output_job = output_job.join(job)
            j_end = time.time()
            join_time += j_end - j_start

    if batch:
        for job in jobs:
            if output_job is None:
                output_job = job
            else:
                j_start = time.time()
                output_job = output_job.join(job)
                j_end = time.time()
                join_time += j_end - j_start
    elif not batch and jobs is not None:
        if output_job is None:
            output_job = jobs
        else:
            j_start = time.time()
            output_job = output_job.join(jobs)
            j_end = time.time()
            join_time += j_end - j_start


    end = time.time()

    sys_time = end - start
    avail_time = sys_time * len(procs) # Available compute time is relative to the total resources used

    print "DJR: End {} Size {} Count {} Time {}".format(end, size, count, time_limit)
    return (output_job, sys_time, avail_time, split_time, join_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='multicore_job_driver.py')

    parser.add_argument('input', type=str, help='JSON file input job description')
    parser.add_argument('output', type=str, help='JSON file output destination')
    parser.add_argument('output_tar', type=str, help='JSON file output files tar destination')
    parser.add_argument('size', type=int, help='Size of sub-job partitions')
    parser.add_argument('count', type=int, help='Number of sub-job partitions')
    parser.add_argument('time', type=int, help='Time allowed for execution')
    parser.add_argument('--cores', type=int, help='Cores used for execution', default=None)
    args = parser.parse_args()

    job = None
    with open(args.input, 'r') as job_desc:
        job_json = json.load(job_desc)
        job = Job.from_description(job_json)

    print "Running {}".format(str(job))

    (output_job, sys_time, avail_time, split_time, join_time) = run_jobs(job, args.size, args.count, args.time, cores=args.cores)

    output_json = output_job.to_description()
    report = {}
    report["job"] = output_json
    report["sys_time"] = sys_time
    report["comp_time"] = avail_time
    report["split_time"] = split_time
    report["join_time"] = join_time

    with tarfile.open(args.output_tar, "w") as output_tar:
        for output in output_job.output_files:
            if os.path.isfile(output):
                output_tar.add(output)

    with open(args.output, 'w') as job_output:
        json.dump(report, job_output)



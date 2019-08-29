import sys
import pickle
from DivisibleJob import Job, TaskResult

def run_job(src_job, args=None):
    jobs = [src_job]
    count=5
    step=10

    finished = None

    # job with query 3500 will always fail!
    while jobs:
        job = jobs.pop(0)
        if len(job) > step:
            step = step * 10
            jobs = jobs + (job.split(count = count, step = step))        
        else:
            job.execute()
            failed_specs = job.failed_specs()

            if failed_specs:
                if len(job) == 1:
                    print('\n------------------------------Job {} permanentely failed.\n'.format(job))
                    fatal.append(job)
                else:
                    print('\n------------------------------Bisecting to isolate error.')
                    rescue_jobs = job.split(count = 1, step=len(job)/2)
                    jobs.extend(rescue_jobs)
            else:
                if finished is None:
                    finished = job
                else:
                    finished = finished.join(job)

    for q in finished.specs:
        src_job.specs[0].join(q)

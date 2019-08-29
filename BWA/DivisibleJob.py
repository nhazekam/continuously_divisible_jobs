import os
import io
import math
import errno
from subprocess import check_call, check_output, Popen, PIPE, CalledProcessError
import tempfile
import logging as log
import pickle
import json
import importlib

log.basicConfig(level=log.DEBUG)

class JobSpec(object):
    def split(self, splits):
        raise NotImplementedError()

    def execute(self):
        raise NotImplementedError()

    def join(self, other):
        raise NotImplementedError()

    def to_desc(self, dest):
        raise NotImplementedError()

    def from_desc(dest):
        raise NotImplementedError()

    @property
    def input_files(self):
        raise NotImplementedError()

    @property
    def output_files(self):
        raise NotImplementedError()

    @property
    def lower(self):
        raise NotImplementedError()

    @property
    def upper(self):
        raise NotImplementedError()

    @property
    def completed(self):
        raise NotImplementedError()

    @property
    def success(self):
        raise NotImplementedError()

    @property
    def failure(self):
        raise NotImplementedError()

    @staticmethod
    def contiguous(lower, upper):
        if lower.upper+1 == upper.lower:
            return True
        return False

class Job(object):
    def __init__(self, specs):
        self.specs        = list(specs)
        self.size         = sum(len(q) for q in self.specs)
        log.debug('Created job {}.'.format(self))

    def __len__(self):
        return self.size

    @staticmethod
    def from_description(job_json):
        j_info = dict(job_json)
        specs = []
        module = j_info["module"]
        inst_type = j_info["class"]
        InstanceType = getattr(importlib.import_module(module), inst_type)
        spec_list = list(j_info["specs"])
        for s in spec_list:
            specs.append(InstanceType.from_description(s))
        return Job(specs)

    def to_description(self):
        job = {}
        spec_descs = []
        for s in self.specs:
            spec_descs.append(s.to_description())
        job["specs"] = spec_descs
        job["module"] = self.specs[0].__class__.__module__
        job["class"] =  self.specs[0].__class__.__name__
        return job

    def join(self, other):
        specs = self.specs + other.specs

        specs.sort(key=lambda x: x.lower)
        combined_specs = [specs.pop(0)]
        for q in specs:
            lower = combined_specs.pop()
            combined_specs = combined_specs + lower.join(q)
        job = Job(combined_specs)

        return job

    def uncompleted_specs(self):
        qs = []
        for q in self.specs:
            if not q.completed:
                qs.append(q)
        return qs

    def failed_specs(self):
        qs = []
        for q in self.specs:
            if q.failure:
                qs.append(q)
        return qs

    def successful_specs(self):
        qs = []
        for q in self.specs:
            if q.success:
                qs.append(q)
        return qs

    def categorize_jobs(self):
        uncompleted = None
        u_specs = self.uncompleted_specs()
        if len(u_specs) > 0:
            uncompleted = Job(u_specs)

        failed = None
        f_specs = self.failed_specs()
        if len(f_specs) > 0:
            failed = Job(f_specs)

        successful = None
        s_specs = self.successful_specs()
        if len(s_specs) > 0:
            successful = Job(s_specs)
        return (uncompleted, failed, successful)

    def split(self, step, count = 1):
        if len(self) < step:
            log.warn('Size of job {} less than required for one split {}. Not splitting.'.format(self.size, step))
            return [self]

        log.debug('Creating {} split(s) using step {}.'.format(count, step))

        # List of List of specs for jobs
        qs = self._split_specs(count, step)
        jobs = [ Job(specs=job_specs) for job_specs in qs]

        return jobs


    # Return at most 'count' specs of size at most step, plus any remaining
    # specs unmodified.
    def _split_specs(self, count, step):
        qs  = []
        job = []
        step_r = step
        for q in self.specs:
            if len(qs) >= count:
                job.append(q)
            elif len(q) <= step_r:
                job.append(q)
                step_r -= len(q)
            else:
                limits = []
                #limits.append(q.lower+step_r -1)
                limits.extend([l for l in range(q.lower +step_r - 1, min((q.lower+(count*step)), q.upper), step)])
                splits = q.split(limits)
                for sub_q in splits:
                    job.append(sub_q)
                    step_r -= len(sub_q)
                    if step_r == 0:
                        qs.append(job)
                        job = []
                        step_r = step

            if step_r == 0:
                qs.append(job)
                job = []
                step_r = step

        if len(job) > 0:
            qs.append(job)
        log.debug('{} specs in, {} specs out'.format(len(self.specs), len(qs)))
        return qs

    def execute(self):
        log.debug('Executing job {}.'.format(self))
        for q in self.specs:
            if q.completed:
                continue
            try:
                q.execute()
            except Exception as e:
                log.debug('Encountered Exception: {}'.format(e))

    @property
    def input_files(self):
		input_set = set()
		for spec in self.specs:
			for files in (spec.input_files):
				input_set.add(files)
		return list(input_set)

    @property
    def output_files(self):
		output_set = set()
		for spec in self.specs:
			for files in (spec.output_files):
				output_set.add(files)
		return list(output_set)

    def __str__(self):
        return "{}".format('_'.join([str(q) for q in self.specs]))


class TaskResult(object):
    def __init__(self, success, **kwargs):
        self.success = success
        self.data    = dict(kwargs)

    def __bool__(self):
        return bool(self.success)

    def to_description(self):
        result = {}
        result["success"] = self.success
        result["data"] = self.data
        return result

    @staticmethod
    def from_description(json):
        r_info = dict(json)
        result = TaskResult(r_info["success"])
        result.data = r_info["data"]
        return result


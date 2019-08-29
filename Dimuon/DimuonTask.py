from DivisibleJob import JobSpec, TaskResult
import json
import os
import sys
import traceback
import logging as log
from subprocess import check_call, check_output, Popen, PIPE, CalledProcessError

class DimuonTask(JobSpec):
    def __init__(self, event_file, output_file, input_files = [], environment = {}, loffset = 0, lrange = None, repeat = 1):
        self.event_file  = event_file
        self.output_file = output_file
        self._input_files = input_files
        self.environment = environment
        self.loffset     = loffset
        self.repeat      = repeat

        self.result      = None

        maxrange    = max(0, event_file.maxend - loffset)
        self.lrange = min(lrange or maxrange, maxrange)

        self.muon_cols   = ['Muon_pt', 'Muon_eta', 'Muon_phi', 'Muon_mass', 'Muon_charge', 'Muon_mediumId']

    def __contains__(self, idx):
        return (self.loffset <= idx and idx < (self.loffset + self.lrange))

    def __len__(self):
        return self.lrange

    def __hash__(self):
        return hash(('DimuonTask', hash(self.events)))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __str__(self):
        return '{}-{}'.format(self.lower, self.upper)

    def __repr__(self):
        return str(self)

    def cleanup(self):
        os.unlink(self.output_file)

    def split(self, limits):
        limits = list(limits)
        if len(limits) < 1:
            return [self]

        subtasks = []
        lower    = self.loffset
        for upper in limits:
            if upper < lower:
                raise TypeError("upper limit {} must be larger than lower limit {}".format(upper, lower))
            lrange   = upper - lower + 1
            new_output = "{}.{}_{}".format(self.output_file, lower, upper)
            new_task = DimuonTask(self.event_file, new_output, self.input_files, self.environment, lower, lrange)
            subtasks.append(new_task)
            lower = upper
        upper = (self.loffset + self.lrange)
        if lower < upper:
            lrange   = upper - lower + 1
            new_output = "{}.{}_{}".format(self.output_file, lower, upper)
            new_task = DimuonTask(self.event_file, new_output, self.input_files, self.environment, lower, lrange)
            subtasks.append(new_task)
        return subtasks

    def execute(self):
        log.debug('Executing dimoun detection {}.'.format(self))
        try:
            cmd = ['./task.py', self.event_file.__class__.__name__, self.event_file.root_file, self.output_file, str(self.loffset), str(self.lrange), str(self.repeat)] + self.muon_cols

            p = Popen(cmd, stdout=PIPE, stderr=PIPE)
            stdout, stderr = p.communicate()
            log.debug(('Exit status of dimuon detection {}: {}').format(self, p.returncode))
            if p.returncode >= 0:
                log.debug(('STDOUT: {} \n\nSTDERR: {}\n').format(stdout, stderr))
            self.result = TaskResult(success=p.returncode == 0, exit_status=p.returncode, stdout=stdout, stderr=stderr)
        except Exception as e:
            log.warning('Could not execute dimuon detection {}: {}'.format(self, e))
            if p:
                p.terminate()
            self.result = TaskResult(success=False, error=e)
        finally:
            return self.result


#'''
#        log.debug('Executing dimoun detection {}.'.format(self))
#        try:
#            check_call(['./task.py', self.event_file.__class__.__name__, self.event_file.root_file, self.output_file, str(self.loffset), str(self.lrange), str(self.repeat)] + self.muon_cols)
#
#            self.result = TaskResult(success = True)
#        except Exception as e:
#            traceback.print_exc()
#            log.warning('Could not execute dimuon detection {}: {}'.format(self, e))
#            self.result = TaskResult(success = False)
#        finally:
#            return self.result
#'''

    def join(self, other):
        # merging can be done in any order, so we simply return a list
        return [self, other]

    @property
    def input_files(self):
        return self._input_files

    @property
    def output_files(self):
        return [self.output_file]

    @property
    def lower(self):
        return self.loffset

    # range: [self.lower, self.upper)
    @property
    def upper(self):
        return self.loffset + self.lrange

    @property
    def completed(self):
        return bool(self.result)

    @property
    def success(self):
        return self.completed and self.result.success

    @property
    def failure(self):
        return self.completed and not self.result.success


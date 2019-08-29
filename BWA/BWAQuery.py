import os, io, math, errno, pickle, json
from subprocess import check_call, check_output, Popen, PIPE, CalledProcessError
import tempfile, logging as log
from VirtualFile import FastqFile
from DivisibleJob import JobSpec, TaskResult
log.basicConfig(level=log.DEBUG)

class BWAQuerySimple(JobSpec):

    def __init__(self, data_file, output_file, input_files, output_files, environment, loffset=0, lrange=None, parents=[]):
        self.data_file = data_file
        self.parents = parents
        self.output_file = output_file
        self.static_input_files = input_files
        self.static_output_files = output_files
        self.environment = dict(environment)
        self.loffset = loffset
        if lrange is None:
            self.lrange = int(int(check_output(['/bin/wc', '-l', data_file]).split()[0]) / 4)
        else:
            self.lrange = lrange
        self.result = None
        return

    def verify_data(self):
        data_exists = os.path.isfile(self.data_file)
        while not data_exists:
            if len(self.parents) == 0:
                raise RuntimeWarning(('Job data file does not exist and cannot be recovered : {}').format(self.data_file))
            parent, p_loffset, p_lrange = self.parents.pop()
            if not os.path.isfile(parent):
                continue
            f_in = io.open(parent, 'r', encoding='ascii')
            for entry in range(0, (self.loffset - p_loffset) * 4):
                f_in.readline()

            with io.open(self.data_file, 'w', encoding='ascii') as (sub_f_in):
                for entry in range(0, self.lrange * 4):
                    sub_f_in.write(f_in.readline())

            f_in.close()
            data_exists = os.path.isfile(self.data_file)
            self.parents.append((parent, p_loffset, p_lrange))
            print ('Recovered data file {} from {}').format(self.data_file, parent)

    def to_description(self):
        query = {}
        query['data_file'] = self.data_file
        query['output_file'] = self.output_file
        query['static_input_files'] = list(self.static_input_files)
        query['static_output_files'] = list(self.static_output_files)
        query['environment'] = dict(self.environment)
        query['loffset'] = self.loffset
        query['lrange'] = self.lrange
        query['parents'] = list(self.parents)
        if self.result:
            query['result'] = self.result.to_description()
        return query

    @staticmethod
    def from_description(json):
        query = dict(json)
        input_files = list(query['static_input_files'])
        output_files = list(query['static_output_files'])
        parents = list(query['parents'])
        q = BWAQuerySimple(query['data_file'], query['output_file'], input_files, output_files, dict(query['environment']), query['loffset'], query['lrange'], parents)
        if query.get('result', None):
            q.result = TaskResult.from_description(query['result'])
        else:
            q.verify_data()
        return q

    def get_data_handle(self):
        f_in = io.open(self.data_file, 'r', encoding='ascii')
        return f_in

    def get_sub_query(self, data_fh, lower, lrange):
        in_file = ('{}_to_{}.fq').format(lower, lower + lrange - 1)
        output_file = ('{}_to_{}.sam').format(lower, lower + lrange - 1)
        with io.open(in_file, 'w', encoding='ascii') as (sub_f_in):
            for entry in range(0, lrange * 4):
                sub_f_in.write(data_fh.readline())

        parents = self.parents
        parents.append((self.data_file, self.loffset, self.lrange))
        return BWAQuerySimple(in_file, output_file, self.static_input_files, self.static_output_files, self.environment, lower, lrange, parents)

    def split(self, limits):
        limits = list(limits)
        if len(limits) < 1:
            return [self]
        queries = []
        f_in = self.get_data_handle()
        lower = self.loffset
        coverage = self.lrange
        for upper in limits:
            if upper < lower:
                raise TypeError(('upper limit {} must be larger than lower limit {}').format(upper, lower))
            lrange = upper - lower + 1
            query = self.get_sub_query(f_in, lower, lrange)
            queries.append(query)
            coverage = coverage - lrange
            lower = upper + 1

        queries.append(self.get_sub_query(f_in, lower, coverage))
        if f_in:
            f_in.close()
        return queries

    def get_command(self):
        db = self.environment['reference']
        return ('./bwa mem {ref} {query} -o {output} {header}').format(query=self.data_file, ref=db, output=self.output_file, header='-g' if self.loffset > 0 else '')

    def execute(self):
        try:
            log.debug(('Executing BWA query {}.').format(self))
            cmd = self.get_command()
            log.debug(('Executing BWA query {}.').format(cmd))
            p = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
            stdout, stderr = p.communicate()
            log.debug(('Exit status of BWA query {} is {}.').format(self, p.returncode))
            if p.returncode >= 0:
                log.debug(('STDOUT: {} \n\nSTDERR: {}\n').format(stdout, stderr))
            self.result = TaskResult(success=p.returncode == 0, exit_status=p.returncode, stdout=stdout, stderr=stderr)
        except Exception as e:
            log.warning(('Could not execute BWA query {}: {}').format(self, e))
            if p:
                p.terminate()
            self.result = TaskResult(success=False, error=e)
        finally:
            return self.result

    def comp(self, other):
        if self.lower < other.lower and self.upper < other.lower:
            return (self, other)
        if other.lower < self.lower and other.upper < self.lower:
            return (other, self)
        log.debug(('Overlapping Queries {} -> {}').format(str(self), str(other)))
        exit(4)
        return [
         self, other]

    def contiguous(self, other):
        if self.upper + 1 == other.lower:
            return True
        return False

    def shared_parents(self, other):
        parents = []
        for i in range(0, min(len(self.parents), len(other.parents))):
            if other.parents[i] == other.parents[i]:
                parents.append(self.parents[i])
            else:
                break

        return parents

    def join_unrun(self, lower, upper):
        data_file = ('{}_to_{}.fq').format(lower.lower, upper.upper)
        output_file = ('{}_to_{}.sam').format(lower.lower, upper.upper)
        f_lower = io.open(lower.data_file, 'a', encoding='ascii')
        with io.open(upper.data_file, 'r', encoding='ascii') as (f_upper):
            f_lower.write(f_upper.read())
        parents = lower.shared_parents(upper)
        f_lower.close()
        os.rename(self.data_file, data_file)
        os.unlink(upper.data_file)
        return [
         BWAQuerySimple(data_file, output_file, self.static_input_files, self.static_output_files, self.environment, lower.lower, upper.lrange + lower.lrange, parents)]

    def join_complete(self, lower, upper):
        data_file = ('{}_to_{}.fq').format(lower.lower, upper.upper)
        output_file = ('{}_to_{}.sam').format(lower.lower, upper.upper)
        f_lower = io.open(lower.output_file, 'a', encoding='ascii')
        with io.open(upper.output_file, 'r', encoding='ascii') as (f_upper):
            line = f_upper.readline()
            while line:
                f_lower.write(line)
                line = f_upper.readline()

        os.rename(lower.output_file, output_file)
        if lower.data_file and os.path.isfile(lower.data_file):
            os.unlink(lower.data_file)
        if upper.data_file and os.path.isfile(upper.data_file):
            os.unlink(upper.data_file)
        os.unlink(upper.output_file)
        parents = lower.shared_parents(upper)
        query = BWAQuerySimple(data_file, output_file, self.static_input_files, self.static_output_files, self.environment, lower.lower, upper.lrange + lower.lrange, parents)
        query.result = lower.result
        return [
         query]

    def join(self, other):
        lower, upper = self.comp(other)
        if not lower.contiguous(upper):
            return [lower, upper]
        if lower.result is None and upper.result is None:
            return self.join_unrun(lower, upper)
        if lower.result is None or upper.result is None:
            return [lower, upper]
        if not (lower.result.success and upper.result.success):
            return [lower, upper]
        if lower.lower > 0:
            return [lower, upper]
        return self.join_complete(lower, upper)

    def __contains__(self, n):
        return self.loffset <= n and n < self.loffset + self.lrange

    def __len__(self):
        return self.lrange

    def __hash__(self):
        return hash(self.data_file)

    def __eq__(self, other):
        return self.loffset == other.loffset and self.lrange == other.lrange and self.data_file == other.data_file

    def __str__(self):
        return ('{}-{}').format(self.lower, self.upper)

    @property
    def input_files(self):
        inputs = []
        inputs = inputs + self.static_input_files
        inputs.append(self.data_file)
        return inputs

    @property
    def output_files(self):
        outputs = []
        outputs = outputs + self.static_output_files
        outputs.append(self.output_file)
        return outputs

    @property
    def lower(self):
        return self.loffset

    @property
    def upper(self):
        return self.loffset + self.lrange - 1

    @property
    def completed(self):
        if self.result is None:
            return False
        return True

    @property
    def success(self):
        if self.result and self.result.success:
            return True
        return False

    @property
    def failure(self):
        if self.result and not self.result.success:
            return True
        return False


class BWAQueryIndexVF(BWAQuerySimple):

    def __init__(self, data_file, output_file, input_files, output_files, environment, loffset=0, lrange=None):
        self.data_file = data_file
        self.output_file = output_file
        self.static_input_files = input_files
        self.static_output_files = output_files
        self.environment = dict(environment)
        self.loffset = loffset
        if lrange is None:
            self.lrange = data_file.entries
        else:
            self.lrange = lrange
        self.result = None
        return

    @property
    def input_files(self):
        inputs = []
        inputs = inputs + self.static_input_files
        inputs.append(self.data_file)
        inputs.append(self.data_file.index)
        return inputs

    def to_description(self):
        self.data_file = self.data_file.create_sub_data(self.loffset, self.lrange)
        query = {}
        query['data_file'] = self.data_file.to_description()
        query['output_file'] = self.output_file
        query['static_input_files'] = self.static_input_files
        query['static_output_files'] = self.static_output_files
        query['environment'] = self.environment
        query['loffset'] = self.loffset
        query['lrange'] = self.lrange
        if self.result:
            query['result'] = self.result.to_description()
        return query

    @staticmethod
    def from_description(json_doc):
        query = dict(json_doc)
        data_source = FastqFile.from_description(dict(query['data_file']))
        input_files = list(query['static_input_files'])
        output_files = list(query['static_output_files'])
        q = BWAQueryIndexVF(data_source, query['output_file'], input_files, output_files, query['environment'], query['loffset'], query['lrange'])
        if query.get('result', None):
            q.result = TaskResult.from_description(query['result'])
        return q

    def get_data_handle(self):
        return

    def get_sub_query(self, data_fh, lower, lrange):
        output_file = ('{}_to_{}.sam').format(lower, lower + lrange - 1)
        return BWAQueryIndexVF(self.data_file, output_file, self.static_input_files, self.static_output_files, self.environment, lower, lrange)

    def get_command(self):
        db = self.environment['reference']
        query_name = self.data_file.data_name
        return ('./bwa mem -e {offset} -l {limit} {ref} {query} -o {output} {header}').format(offset=self.data_file.offset(self.loffset), limit=self.lrange, query=query_name, ref=db, output=self.output_file, header='-g' if self.loffset > 0 else '')

    def join_unrun(self, lower, upper):
        output_file = ('{}_to_{}.sam').format(lower.lower, upper.upper)
        return [
         BWAQueryIndexVF(lower.data_file, output_file, self.static_input_files, self.static_output_files, self.environment, lower.lower, lower.lrange + upper.lrange)]

    def join_complete(self, lower, upper):
        output_file = ('{}_to_{}.sam').format(lower.lower, upper.upper)
        f_lower = io.open(lower.output_file, 'a', encoding='ascii')
        with io.open(upper.output_file, 'r', encoding='ascii') as (f_upper):
            line = f_upper.readline()
            while line:
                f_lower.write(line)
                line = f_upper.readline()

        f_lower.close()
        os.rename(lower.output_file, output_file)
        os.unlink(upper.output_file)
        query = BWAQueryIndexVF(lower.data_file, output_file, self.static_input_files, self.static_output_files, self.environment, lower.lower, lower.lrange + upper.lrange)
        query.result = lower.result
        return [
         query]


class BWAQuerySimpleByte(BWAQuerySimple):

    def __init__(self, data_file, output_file, input_files, output_files, environment, loffset=0, lrange=None, byte_offset=0):
        self.data_file = data_file
        self.output_file = output_file
        self.static_input_files = input_files
        self.static_output_files = output_files
        self.environment = dict(environment)
        self.byte_offset = byte_offset
        self.loffset = loffset
        if lrange is None:
            self.lrange = int(int(check_output(['/bin/wc', '-l', data_file]).split()[0]) / 4)
        else:
            self.lrange = lrange
        self.result = None
        return

    def get_data_handle(self):
        f_in = io.open(self.data_file, 'r', encoding='ascii')
        return f_in

    def get_sub_query(self, data_fh, lower, lrange):
        output_file = ('{}_to_{}.sam').format(lower, lower + lrange - 1)
        byte_offset = data_fh.tell()
        for entry in range(0, lrange * 4):
            data_fh.readline()

        return BWAQuerySimpleByte(self.data_file, output_file, self.static_input_files, self.static_output_files, self.environment, lower, lrange, byte_offset)

    def get_command(self):
        db = self.environment['reference']
        return ('./bwa mem -e {bo} -l {limit}  {ref} {query} -o {output} {header}').format(query=self.data_file, ref=db, output=self.output_file, bo=self.byte_offset, limit=self.lrange, header='-g' if self.loffset > 0 else '')

    def join_unrun(self, lower, upper):
        output_file = ('{}_to_{}.sam').format(lower.lower, upper.upper)
        return [
         BWAQuerySimpleByte(lower.data_file, output_file, self.static_input_files, self.static_output_files, self.environment, lower.lower, upper.lrange + lower.lrange, lower.byte_offset)]

    def join_complete(self, lower, upper):
        output_file = ('{}_to_{}.sam').format(lower.lower, upper.upper)
        f_lower = io.open(lower.output_file, 'a', encoding='ascii')
        with io.open(upper.output_file, 'r', encoding='ascii') as (f_upper):
            line = f_upper.readline()
            while line:
                f_lower.write(line)
                line = f_upper.readline()

        f_lower.close()
        os.rename(lower.output_file, output_file)
        os.unlink(upper.output_file)
        query = BWAQuerySimpleByte(lower.data_file, output_file, self.static_input_files, self.static_output_files, self.environment, lower.lower, upper.lrange + lower.lrange, lower.byte_offset)
        query.result = lower.result
        return [
         query]


class BWAQueryLogicalVF(BWAQuerySimple):

    def get_data_handle(self):
        return

    def get_sub_query(self, f_in, lower, lrange):
        output_file = ('{}_to_{}.sam').format(lower, lower + lrange - 1)
        return BWAQueryLogicalVF(self.data_file, output_file, self.static_input_files, self.static_output_files, self.environment, lower, lrange)

    def get_command(self):
        db = self.environment['reference']
        return ('./bwa mem -J {offset} -l {limit} {ref} {query} -o {output} {header}').format(offset=self.loffset, limit=self.lrange, query=self.data_file, ref=db, output=self.output_file, header='-g' if self.loffset > 0 else '')

    @staticmethod
    def join_unrun(self, lower, upper):
        output_file = ('{}_to_{}.sam').format(lower, upper)
        return [
         BWAQueryLogicalVF(lower.data_file, output_file, self.static_input_files, self.static_output_files, self.environment, lower.lower, lrange)]

    def join_complete(self, lower, upper):
        output_file = ('{}_to_{}.sam').format(lower, upper)
        f_lower = io.open(lower.output_file, 'a', encoding='ascii')
        with io.open(upper.output_file, 'r', encoding='ascii') as (f_upper):
            line = f_upper.readline()
            while line:
                if not line.startswith('@'):
                    f_lower.write(line)
                line = f_upper.readline()

        f_lower.close()
        os.rename(lower.output_file, output_file)
        os.unlink(upper.output_file)
        query = BWAQueryLogicalVF(lower.data_file, output_file, self.static_input_files, self.static_output_files, self.environment, lower.lower, lower.lrange + upper.lrange)
        query.result = lower.result
        return [
         query]

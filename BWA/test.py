# uncompyle6 version 3.3.2
# Python bytecode 2.7 (62211)
# Decompiled from: Python 2.7 (r27:82500, Dec 20 2016, 14:19:15) 
# [GCC 4.4.7 20120313 (Red Hat 4.4.7-17)]
# Embedded file name: /scratch365/nhazekam/partitioning-scalability/continuous_jobs/BWA/VirtualFile.py
# Compiled at: 2019-05-15 09:32:27
import os, io, math, errno
from subprocess import check_call, check_output, Popen, PIPE, CalledProcessError
import tempfile, logging as log, struct
log.basicConfig(level=log.DEBUG)

class VirtualFile(object):

    def __init__(self, data_name, index, entries, size, start_byte=0):
        self.data_source = data_name
        self.data_name = data_name
        self.index = index
        self.start_byte = start_byte
        self.end_byte = size
        self.index_width = 8
        self.entries = entries

    def read_number_at(self, f_in, offset):
        location = offset * self.index_width
        f_in.seek(location)
        return struct.unpack('Q', f_in.read(self.index_width))[0]

    def offset(self, loffset):
        with open(self.index, 'r') as (f_in):
            offset = self.read_number_at(f_in, loffset) - self.start_byte
            return offset

    def range(self, loffset, lrange):
        with open(self.index, 'r') as (f_in):
            return self.read_number_at(f_in, loffset + lrange) - self.start_byte

    def offset_and_range(self, loffset, lrange):
        with open(self.index, 'r') as (f_in):
            offset = self.read_number_at(f_in, loffset) - self.start_byte
            range = self.read_number_at(f_in, lrange) - self.start_byte
            return (
             offset, range)

    def to_description(self):
        raise NotImplementedError

    @staticmethod
    def from_description(json):
        raise NotImplementedError

    @staticmethod
    def from_file(data_name):
        raise NotImplementedError

    @staticmethod
    def index(data_name, index_name):
        raise NotImplementedError


class FastqFile(VirtualFile):

    def __init__(self, data_file, index, entries, size, start_byte=0, data_name=None):
        self.data_source = data_file
        if data_name:
            self.data_name = data_name
        else:
            self.data_name = data_file
        self.index = index
        self.start_byte = start_byte
        self.end_byte = size
        self.index_width = 8
        self.entries = entries

    def create_sub_data(self, loffset, lrange):
        sub_start_byte = self.offset(loffset)
        sub_end_byte = self.offset(loffset + lrange)
        data_name = ('sub_data.{}.{}.{}').format(loffset, loffset + lrange - 1, self.data_name)
        sub = FastqFile(self.data_source, self.index, lrange, sub_end_byte, sub_start_byte, data_name)
        return sub

    @staticmethod
    def from_file(data_name):
        entries = int(check_output(['/bin/wc', '-l', data_name]).split()[0]) / 4
        size = os.path.getsize(data_name)
        index_file = ('{}.{}').format(data_name, 'virtual_index')
        FastqFile.index(data_name, index_file)
        return FastqFile(data_name, index_file, entries, size)

    @staticmethod
    def index(data_name, index_file):
        with open(index_file, 'w') as (f_out):
            with open(data_name, 'r') as (f_in):
                line = 0
                f_out.write(struct.pack('Q', 0))
                while f_in.readline():
                    line += 1
                    if line % 4 == 0:
                        position = f_in.tell()
                        f_out.write(struct.pack('Q', position))

                position = f_in.tell()
                f_out.write(struct.pack('Q', position))

    def to_description(self):
        f_info = {}
        f_info['data_source'] = self.data_source
        f_info['data_name'] = self.data_name
        f_info['index'] = self.index
        f_info['start_byte'] = self.start_byte
        f_info['end_byte'] = self.end_byte
        f_info['index_width'] = self.index_width
        f_info['entries'] = self.entries
        return f_info

    @staticmethod
    def from_description(json):
        f_info = dict(json)
        fastq = FastqFile(f_info['data_source'], f_info['index'], f_info['entries'], f_info['end_byte'], f_info['start_byte'], f_info['data_name'])
        return fastq

    def __str__(self):
        return self.data_name
# okay decompiling VirtualFile.pyc

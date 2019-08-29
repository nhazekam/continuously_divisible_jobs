import os
import io
import math
import errno
from subprocess import check_call, check_output, Popen, PIPE, CalledProcessError
import tempfile
import logging as log
import struct

log.basicConfig(level=log.DEBUG)


class VirtualFile(object):
    def __init__(self, data_name, index, entries, size, start_byte=0):
        self.data_source = data_name
        self.data_name = data_name
        self.index = index
        self.start_byte = start_byte
        self.end_byte = size
        self.index_width = 8                  # 8 bytes per index
        self.entries = entries
    
    def read_number_at(self, f_in, offset):
        location = (offset * self.index_width)
        f_in.seek(location)
        return struct.unpack('Q', f_in.read(self.index_width))[0]

    def offset(self, loffset):
        with open(self.index, 'r') as f_in:
            offset = self.read_number_at(f_in, loffset)-self.start_byte
            return offset


    def range(self, loffset, lrange):
        with open(self.index, 'r') as f_in:
            return self.read_number_at(f_in, loffset + lrange)-self.start_byte

    def offset_and_range(self, loffset, lrange):
        with open(self.index, 'r') as f_in:
            offset = self.read_number_at(f_in, loffset)-self.start_byte
            range  = self.read_number_at(f_in, lrange)-self.start_byte
            return (offset, range)

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
    def __init__(self, data_file, index, entries, size, start_byte=0, parent=None):
        self.data_name = data_file
        if parent:
            self.parent = parent
        else:
            self.parent = None
        self.index = index
        self.start_byte = start_byte
        self.end_byte = size
        self.index_width = 8                  # 8 bytes per index
        self.entries = entries
    
    def create_sub_data(self, loffset, lrange):
        sub_start_byte = self.offset(loffset)
        sub_end_byte = self.offset(loffset+ lrange)
        if sub_start_byte == self.start_byte and sub_end_byte == self.end_byte:
            return self
        data_name = "sub_data.{}.{}.{}".format(loffset, loffset+lrange-1, self.data_name)
        if not os.path.isfile(self.data_name):
            return FastqFile(data_name, self.index, lrange, sub_end_byte, sub_start_byte, self.parent)
        return FastqFile(data_name, self.index, lrange, sub_end_byte, sub_start_byte, self)

    @staticmethod
    def from_file(data_name, reuse_index=False):
        entries = int(check_output(['/bin/wc', '-l', data_name]).split()[0])/4
        size = os.path.getsize(data_name)
        index_file = "{}.{}".format(data_name, "virtual_index")

        # Only index if it doesn't exist...
        if not (reuse_index and os.path.isfile(index_file)):
            FastqFile.index(data_name, index_file)

        return FastqFile(data_name, index_file, entries, size)

    @staticmethod
    def index(data_name, index_file):
        with open(index_file, 'w') as f_out:
            with open(data_name, 'r') as f_in:
                line = 0
                f_out.write(struct.pack('Q', 0))
                while f_in.readline():
                    line += 1
                    if line % 4 == 0:
                        position = f_in.tell()
                        f_out.write(struct.pack('Q', position))
                # Write the last file locations as a backstop
                position = f_in.tell()
                f_out.write(struct.pack('Q', position))

    def to_description(self):
        f_info = {}
        if self.parent:
            f_info["parent"] = self.parent.to_description()
        f_info["data_name"] = self.data_name
        f_info["index"] = self.index
        f_info["start_byte"] = self.start_byte
        f_info["end_byte"] = self.end_byte
        f_info["index_width"] = self.index_width
        f_info["entries"] = self.entries
        return f_info

    @staticmethod
    def from_description(json, initial=True):
        f_info = dict(json)
        parent = f_info.get('parent', None)
        if parent:
            parent = FastqFile.from_description(parent, False)
        data_file = f_info['data_name']
        if initial and not os.path.isfile(data_file):
            while not os.path.isfile(parent.data_name):
                parent = parent.parent
        fastq = FastqFile(
            data_file, 
            f_info['index'], 
            f_info['entries'], 
            f_info['end_byte'], 
            f_info['start_byte'],
            parent
        )
        return fastq

    def __str__(self):
        return self.data_name

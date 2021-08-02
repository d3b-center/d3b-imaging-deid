# Extracted verbatim from
# https://github.com/bgilbert/anonymize-slide/blob/31efede655ab09fd0b737621dfc08049cefde3cc/anonymize_slide.py

#  Copyright (c) 2007-2013 Carnegie Mellon University
#  Copyright (c) 2011      Google, Inc.
#  Copyright (c) 2014      Benjamin Gilbert
#  All rights reserved.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of version 2 of the GNU General Public License as
#  published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor,
#  Boston, MA 02110-1301 USA.
#
# Modified by Chaim Reach:
# - updated to Python3
# - added support for Ventana tiffs
# - added option for importing as a python module.

import struct

DEBUG = False

# TIFF types
BYTE = 1
ASCII = 2
SHORT = 3
LONG = 4
FLOAT = 11
DOUBLE = 12
LONG8 = 16

# TIFF tags
IMAGE_DESCRIPTION = 270
STRIP_OFFSETS = 273
STRIP_BYTE_COUNTS = 279
NDPI_MAGIC = 65420
NDPI_SOURCELENS = 65421
XMLPACKET = 700


class UnrecognizedFile(Exception):
    pass


class TiffFile():
    def __init__(self, path):
        # file.__init__(self, path, 'r+b')
        self.file = open(path, 'r+b')

        # Check header, decide endianness
        endian = self.read(2)
        if endian == b'II':
            self._fmt_prefix = '<'
        elif endian == b'MM':
            self._fmt_prefix = '>'
        else:
            raise UnrecognizedFile

        # Check TIFF version
        self._bigtiff = False
        self._ndpi = False
        version = self.read_fmt('H')
        if version == 42:
            pass
        elif version == 43:
            self._bigtiff = True
            magic2, reserved = self.read_fmt('HH')
            if magic2 != 8 or reserved != 0:
                raise UnrecognizedFile
        else:
            raise UnrecognizedFile

        # Read directories
        self.directories = []
        while True:
            in_pointer_offset = self.tell()
            directory_offset = self.read_fmt('D')
            if directory_offset == 0:
                break
            self.seek(directory_offset)
            directory = TiffDirectory(self, len(self.directories),
                    in_pointer_offset)
            if not self.directories and not self._bigtiff:
                # Check for NDPI.  Because we don't know we have an NDPI file
                # until after reading the first directory, we will choke if
                # the first directory is beyond 4 GB.
                if NDPI_MAGIC in directory.entries:
                    if DEBUG:
                        print('Enabling NDPI mode.')
                    self._ndpi = True
            self.directories.append(directory)
        if not self.directories:
            raise IOError('No directories')

    # TiffFile uses composition to pretend to be a file object for backwards compatibility.
    def read(self, n=-1):
        return self.file.read(n)
    def write(self, s):
        return self.file.write(s)
    def seek(self, offset, whence=0):
        return self.file.seek(offset, whence)
    def tell(self):
        return self.file.tell()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()
    # End of file code

    def _convert_format(self, fmt):
        # Format strings can have special characters:
        # y: 16-bit   signed on little TIFF, 64-bit   signed on BigTIFF
        # Y: 16-bit unsigned on little TIFF, 64-bit unsigned on BigTIFF
        # z: 32-bit   signed on little TIFF, 64-bit   signed on BigTIFF
        # Z: 32-bit unsigned on little TIFF, 64-bit unsigned on BigTIFF
        # D: 32-bit unsigned on little TIFF, 64-bit unsigned on BigTIFF/NDPI
        if self._bigtiff:
            fmt = fmt.translate(str.maketrans('yYzZD', 'qQqQQ'))
        elif self._ndpi:
            fmt = fmt.translate(str.maketrans('yYzZD', 'hHiIQ'))
        else:
            fmt = fmt.translate(str.maketrans('yYzZD', 'hHiII'))
        return self._fmt_prefix + fmt

    def fmt_size(self, fmt):
        return struct.calcsize(self._convert_format(fmt))

    def near_pointer(self, base, offset):
        # If NDPI, return the value whose low-order 32-bits are equal to
        # @offset and which is within 4 GB of @base and below it.
        # Otherwise, return offset.
        if self._ndpi and offset < base:
            seg_size = 1 << 32
            offset += ((base - offset) // seg_size) * seg_size
        return offset

    def read_fmt(self, fmt, force_list=False):
        fmt = self._convert_format(fmt)
        vals = struct.unpack(fmt, self.read(struct.calcsize(fmt)))
        # vals = tuple(self.read(struct.calcsize(fmt)*4))
        if len(vals) == 1 and not force_list:
            return vals[0]
        else:
            return vals

    def write_fmt(self, fmt, *args):
        fmt = self._convert_format(fmt)
        self.write(struct.pack(fmt, *args))


class TiffDirectory(object):
    def __init__(self, fh, number, in_pointer_offset):
        self.entries = {}
        count = fh.read_fmt('Y')
        for _ in range(count):
            entry = TiffEntry(fh)
            self.entries[entry.tag] = entry
        self._in_pointer_offset = in_pointer_offset
        self._out_pointer_offset = fh.tell()
        self._fh = fh
        self._number = number

    def delete(self, expected_prefix=None):
        # Get strip offsets/lengths
        try:
            offsets = self.entries[STRIP_OFFSETS].value()
            lengths = self.entries[STRIP_BYTE_COUNTS].value()
        except KeyError:
            raise IOError('Directory is not stripped')

        # Wipe strips
        for offset, length in zip(offsets, lengths):
            offset = self._fh.near_pointer(self._out_pointer_offset, offset)
            if DEBUG:
                print('Zeroing', offset, 'for', length)
            self._fh.seek(offset)
            if expected_prefix:
                buf = self._fh.read(len(expected_prefix))
                if buf != expected_prefix:
                    raise IOError('Unexpected data in image strip')
                self._fh.seek(offset)
            self._fh.write(b'\0' * length)

        # Remove directory
        if DEBUG:
            print('Deleting directory', self._number)
        self._fh.seek(self._out_pointer_offset)
        out_pointer = self._fh.read_fmt('D')
        self._fh.seek(self._in_pointer_offset)
        self._fh.write_fmt('D', out_pointer)


class TiffEntry(object):
    def __init__(self, fh):
        self.start = fh.tell()
        self.tag, self.type, self.count, self.value_offset = \
                fh.read_fmt('HHZZ')
        self._fh = fh

    def format_type(self):
        if self.type == BYTE:
            item_fmt = 'b'
        elif self.type == ASCII:
            item_fmt = 'c'
            # item_fmt = 's'
        elif self.type == SHORT:
            item_fmt = 'H'
        elif self.type == LONG:
            item_fmt = 'I'
        elif self.type == LONG8:
            item_fmt = 'Q'
        elif self.type == FLOAT:
            item_fmt = 'f'
        elif self.type == DOUBLE:
            item_fmt = 'd'
        else:
            raise ValueError('Unsupported type')
        return item_fmt

    def value(self):
        # if self.type == BYTE:
        #     item_fmt = 'b'
        # elif self.type == ASCII:
        #     item_fmt = 'c'
        # elif self.type == SHORT:
        #     item_fmt = 'H'
        # elif self.type == LONG:
        #     item_fmt = 'I'
        # elif self.type == LONG8:
        #     item_fmt = 'Q'
        # elif self.type == FLOAT:
        #     item_fmt = 'f'
        # elif self.type == DOUBLE:
        #     item_fmt = 'd'
        # else:
        #     raise ValueError('Unsupported type')

        item_fmt = self.format_type()

        fmt = '%d%s' % (self.count, item_fmt)

        len = self._fh.fmt_size(fmt)
        if len <= self._fh.fmt_size('Z'):
            # Inline value
            self._fh.seek(self.start + self._fh.fmt_size('HHZ'))
        else:
            # Out-of-line value
            self._fh.seek(self._fh.near_pointer(self.start, self.value_offset))
        items = self._fh.read_fmt(fmt, force_list=True)
        if self.type == ASCII:
            if items[-1] != b'\0':
                raise ValueError('String not null-terminated')
            return b''.join(items[:-1])
        else:
            return items

    def overwrite_entry(self, byte_string):
        """Overwrites self.value with data and destroys the ENTIRE previous entry.
        Extra space will be overwritten.
        WARNING: Currently only supports BYTE and ASCII types."""
        # TYPE = tif.directories[directory].entries[entry].type
        # fmt = f"{tif.directories[directory].entries[entry].count}{self.format_type()}"
        fmt = '%d%s' % (self.count, self.format_type())
        # Hacky fix for strings
        fmt = fmt.replace("c", "s")

        # WARNING: Assumes entry contains a string. May fail if this function is extended to non-string types.
        entry_ordinals = self.value()
        old_value = "".join([chr(x) for x in entry_ordinals])
        # The extra length that we'll need to overwrite with junk data.
        null_pad = len(old_value) - len(byte_string)

        # Write new value
        self._fh.seek(self.value_offset)

        if self.type == ASCII:
            # ASCII uses nulls to divide substrings, so we pad with spaces instead.
            new_value = byte_string + b" " * null_pad
            self._fh.write_fmt(fmt, new_value)
        elif self.type == BYTE:
            new_value = byte_string + b"\0" * null_pad
            self._fh.write_fmt(fmt, *new_value)
        else:
            raise ValueError('Unsupported type')

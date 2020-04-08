# Copyright 2017 Ben Walsh
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import os
import tempfile
import shutil
import numpy as np

if sys.version_info < (3,):
    import cStringIO
    string_io = cStringIO.StringIO

    def string(s='', encoding=None):  # pylint: disable=unused-argument
        return bytes(s)
else:
    import io
    string_io = io.StringIO

    def string(s='', encoding='utf8'):
        return bytes(s, encoding)

SPACE = b' '
COMMA = b','

class TempCsvFile(object):
    def __init__(self, data, bdata=None):
        self._data = bdata or string(data, 'utf8')

    def __enter__(self):
        self._dirname = tempfile.mkdtemp()
        fname = os.path.join(self._dirname, 'test.csv')

        with open(fname, 'wb') as fp:
            fp.write(self._data)

        return fname

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self._dirname)


def array(lst):
    res = np.array(lst)
    if res.dtype.kind == 'U':
        return res.astype('S')
    return res


def set_top_bit(s):
    return string(''.join(chr(ord(c) | 128) for c in s), 'latin1')

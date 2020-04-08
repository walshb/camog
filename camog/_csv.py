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

import multiprocessing

from . import _cfastcsv


def _check_args(sep, headers, nthreads):
    if not isinstance(sep, str):
        raise ValueError('Invalid separator %r' % (sep,))

    if nthreads is None:
        nthreads = multiprocessing.cpu_count()
    elif nthreads <= 0:
        raise ValueError('Invalid nthreads %s' % nthreads)

    nheaders = 1 if headers else 0

    return nthreads, nheaders


def load(filename, sep=',', headers=True, nthreads=None, flags=0, col_to_type=None,
         missing_int_val=0, missing_float_val=0.0):
    if not isinstance(filename, str):
        raise ValueError('Invalid filename %r' % (filename,))

    nthreads, nheaders = _check_args(sep, headers, nthreads)

    return _cfastcsv.parse_file(filename, sep, nthreads, flags,
                                nheaders, missing_int_val, missing_float_val,
                                col_to_type)


def loads(s, sep=',', headers=True, nthreads=None, flags=0, col_to_type=None,
          missing_int_val=0, missing_float_val=0.0):
    nthreads, nheaders = _check_args(sep, headers, nthreads)

    return _cfastcsv.parse_csv(s, sep, nthreads, flags,
                               nheaders, missing_int_val, missing_float_val,
                               col_to_type)

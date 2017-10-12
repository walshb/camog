#!/usr/bin/env python
#
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
import logging

logging.basicConfig(level=logging.DEBUG)

import argparse
import datetime
import tempfile
import shutil

import camog
try:
    import paratext
except ImportError:
    paratext = None
try:
    import pandas as pd
except ImportError:
    pd = None


_logger = logging.getLogger(__name__)


def _check_path(mod):
    _logger.info('%s', mod.__file__)
    assert 'extern_pkgs' in os.path.abspath(mod.__file__)


def _do_timing(times, name, func):
    t0 = datetime.datetime.now()
    func()
    took = (datetime.datetime.now() - t0).total_seconds()

    _logger.info('%s took %.4f s', name, took)

    times.setdefault(name, []).append(took)


def _pandas_textreader_read(fname):
    with open(fname, 'rb') as fp:
        reader = pd._libs.parsers.TextReader(fp, memory_map=True, header=None)
        reader.read()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-n', type=int, default=1000)
    parser.add_argument('--nsamples', type=int, default=10)
    parser.add_argument('--nthreads', type=int, default=4)
    parser.add_argument('--names', type=lambda s: s.split(','),
                        default=['camog', 'pandas', 'paratext'])
    parser.add_argument('--base', default='pandas')

    args = parser.parse_args()

    if 'paratext' in args.names:
        _check_path(paratext)
    if 'pandas' in args.names:
        _check_path(pd)

    data = '123.0,456.0,789.0\n' * args.n

    dirname = tempfile.mkdtemp()
    fname = os.path.join(dirname, 'test.csv')

    with open(fname, 'w') as fp:
        fp.write(data)

    _logger.info('data size ~= %.1f MB', args.n * 3.0 * 8.0 / (1024.0 * 1024.0))

    times = {}
    funcs = {'camog': lambda: camog.load(fname, headers=False, nthreads=args.nthreads)}
    if pd:
        funcs['pandas'] = lambda: _pandas_textreader_read(fname)
    if paratext:
        funcs['paratext'] = lambda: paratext.load_csv_to_dict(fname, num_threads=args.nthreads)

    for i in range(args.nsamples):
        for name in args.names:
            _do_timing(times, name, funcs[name])

    shutil.rmtree(dirname)

    if args.base not in times:
        return 0

    for name in args.names:
        _logger.info('%s is %.1f times faster than %s',
                     name, min(times[args.base]) / min(times[name]), args.base)

    return 0


if __name__ == '__main__':
    sys.exit(main())

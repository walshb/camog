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
import logging

logging.basicConfig(level=logging.DEBUG)

import argparse
import datetime

import camog._cfastcsv as cfastcsv

_logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-n', type=int, default=1000)
    parser.add_argument('--nsamples', type=int, default=10)
    parser.add_argument('--nthreads', type=int, default=4)

    args = parser.parse_args()

    data = '123.0,456.0,789.0\n' * args.n

    _logger.info('data size ~= %.1f MB', args.n * 3.0 * 8.0 / (1024.0 * 1024.0))

    total = 0.0
    for i in xrange(args.nsamples):
        t0 = datetime.datetime.now()

        cols = cfastcsv.parse_csv(data, ',', args.nthreads)[1]

        took = (datetime.datetime.now() - t0).total_seconds()
        total += took

        _logger.debug('parse_csv took %.4f s', took)

        _logger.info('rows = %s cols = %s', len(cols[0]), len(cols))

    res = total / float(args.nsamples)

    _logger.info('average %.4f s', res)

    print res

    return 0


if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python
#
# Copyright 2019 Ben Walsh
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

def _pow(b, n):
    if n >= 0:
        r = (1 << 1024) * (b ** n)
    else:
        r = (1 << 1024) // (b ** -n)

    assert r > (1 << 62)

    c = 62 - 1024
    while r > (1 << 62):
        r >>= 1
        c += 1

    return r, c


def main():
    vals = []
    for i, e in enumerate(range(-340, 310)):
        r, c = _pow(5, e)
        vals.append((i, r, c + e))

    sys.stdout.write('uint64_t pow5[] = {')
    for i, r, s in vals:
        sys.stdout.write('%sL' % r)
        if i < len(vals) - 1:
            sys.stdout.write(',')
        if i % 8 == 7:
            sys.stdout.write('\n')
        else:
            sys.stdout.write(' ')
    sys.stdout.write('};\n')

    sys.stdout.write('int shift5[] = {')
    for i, r, s in vals:
        sys.stdout.write('%s' % s)
        if i < len(vals) - 1:
            sys.stdout.write(',')
        if i % 8 == 7:
            sys.stdout.write('\n')
        else:
            sys.stdout.write(' ')
    sys.stdout.write('};\n')

    sys.stdout.write('double pow2[] = {')
    for i, e in enumerate(range(-1022, 1023)):
        sys.stdout.write('%.17g' % (2.0 ** e,))
        if e < 1022:
            sys.stdout.write(',')
        if i % 8 == 7:
            sys.stdout.write('\n')
        else:
            sys.stdout.write(' ')
    sys.stdout.write('};\n')

    return 0


if __name__ == '__main__':
    sys.exit(main())

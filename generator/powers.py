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

minexpo = 0
maxexpo = 310   # exclusive

sys.stdout.write('long double powers[] = {')
for expo in range(minexpo, maxexpo):
    if expo >= maxexpo - 1:
        sys.stdout.write('INFINITY')
    else:
        sys.stdout.write('1.0e%sL' % expo)
    if expo < maxexpo - 1:
        sys.stdout.write(',')
    if (expo - minexpo) % 8 == 7:
        sys.stdout.write('\n')
    else:
        sys.stdout.write(' ')

sys.stdout.write('};\n')

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
import os
import subprocess

mydir = os.path.dirname(__file__)

with open('powers.h', 'w') as fp:
    subprocess.check_call([sys.executable, os.path.join(mydir, 'powers.py')], stdout=fp)

with open('powers5.h', 'w') as fp:
    subprocess.check_call([sys.executable, os.path.join(mydir, 'powers5.py')], stdout=fp)

subprocess.check_call([sys.executable, os.path.join(mydir, 'parser.py'),
                       '--type-stage', '--nextchar=NEXTCHAR_NOQUOTES',
                       '--label-prefix=noquotes',
                       '--outfilename=parser.h'])

subprocess.check_call([sys.executable, os.path.join(mydir, 'parser.py'),
                       '--type-stage', '--nextchar=NEXTCHAR_INQUOTES',
                       '--label-prefix=inquotes',
                       '--outfilename=parser_inquotes.h'])

subprocess.check_call([sys.executable, os.path.join(mydir, 'parser.py'),
                       '--nextchar=NEXTCHAR2_NOQUOTES',
                       '--label-prefix=noquotes',
                       '--outfilename=parser2.h'])

subprocess.check_call([sys.executable, os.path.join(mydir, 'parser.py'),
                       '--nextchar=NEXTCHAR2_INQUOTES',
                       '--label-prefix=inquotes',
                       '--outfilename=parser2_inquotes.h'])

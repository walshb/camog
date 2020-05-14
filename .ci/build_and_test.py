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
import shutil
import glob


_TOPDIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# With virtual envs, sys.executable is the interpreter with symlinks resolved.
# But we need the unresolved bin directory. Assume PATH will take us to it.
_PYTHON = os.path.basename(sys.executable)


def _run(cmd, env_vars={}, **kwargs):
    env = os.environ.copy()
    env.update(env_vars)
    sys.stderr.write('%s\n' % (cmd,))
    sys.stderr.flush()
    subprocess.check_call(cmd, env=env, **kwargs)


def _rm(dirname):
    try:
        shutil.rmtree(dirname)
    except OSError:
        pass


def _clean():
    os.chdir(_TOPDIR)

    _rm(os.path.join('ctests', 'build'))
    _rm('gensrc')
    _rm('build')
    _rm('.cache')
    _rm(os.path.join('tests', '__pycache__'))


def _build_and_test(cflags):
    _clean()

    os.makedirs('gensrc')
    _run([_PYTHON, os.path.join(_TOPDIR, 'generator', 'generate.py')], cwd='gensrc')

    os.chdir('ctests')

    os.makedirs('build')
    os.chdir('build')

    if sys.platform == 'win32':
        _run(['cmake', '..', '-G', 'NMake Makefiles', '-DCMAKE_VERBOSE_MAKEFILE=ON',
              '-DCMAKE_BUILD_TYPE=Release', '-DTHREADSAFE=true'],
             {'CFLAGS': cflags})

        _run(['nmake', 'VERBOSE=1'])

        _run(['test_floats.exe'])
    else:
        _run(['cmake', '..', '-DCMAKE_VERBOSE_MAKEFILE=ON', '-DCMAKE_BUILD_TYPE=Release',
              '-DTHREADSAFE=true'],
             {'CFLAGS': cflags})

        _run(['make', 'VERBOSE=1'])

        _run(['./test_floats'])

    _clean()

    _run([_PYTHON, 'setup.py', 'build'], {'CFLAGS': cflags})

    libdir = os.path.abspath(glob.glob('build' + os.sep + 'lib*')[0])

    os.chdir('tests')

    _run([_PYTHON, '-m', 'pytest', '-sv', '.'], {'PYTHONPATH': libdir})

    _clean()


def main():
    if sys.platform == 'win32':
        _build_and_test('')
    else:
        cflags = '-Wall -Werror -Wsign-compare -Wstrict-prototypes -Wstrict-aliasing=0 -Werror=declaration-after-statement'

        _build_and_test(cflags)
        _build_and_test(cflags + ' -DNO_LONG_DOUBLE')

    return 0


if __name__ == '__main__':
    sys.exit(main())

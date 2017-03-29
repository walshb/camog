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

.PHONY:	all parser clean test benchmark

all:	parser
	CFLAGS='' python setup.py build

parser:
	mkdir -p gensrc
	python ./generator/powers.py >gensrc/powers.i
	python ./generator/parser.py --nextchar=NEXTCHAR_NOQUOTES --label-prefix=noquotes >gensrc/parser.i
	python ./generator/parser.py --nextchar=NEXTCHAR_INQUOTES --label-prefix=inquotes >gensrc/parser_inquotes.i

clean:
	rm -rf $$(find . \( -name '*.so' -o -name '__pycache__' \) -print) gensrc build .cache

test:	all
	export PYTHONPATH=$$(readlink -f $(CURDIR)/build/lib*); cd tests; python -m pytest --pdb -sv test_fastcsv.py test_headers.py test_edge.py test_file.py test_api.py test_format.py test_numbers.py

benchmark:	all
	export PYTHONPATH=$$(readlink -f $(CURDIR)/build/lib*); cd benchmarks; ./many_doubles.py -n 20000000 --nthreads=4

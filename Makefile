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
	python ./generator/powers.py >gensrc/powers.h
	python ./generator/parser.py --type-stage --nextchar=NEXTCHAR_NOQUOTES --label-prefix=noquotes >gensrc/parser.h
	python ./generator/parser.py --type-stage --nextchar=NEXTCHAR_INQUOTES --label-prefix=inquotes >gensrc/parser_inquotes.h
	python ./generator/parser.py --nextchar=NEXTCHAR2_NOQUOTES --label-prefix=noquotes >gensrc/parser2.h
	python ./generator/parser.py --nextchar=NEXTCHAR2_INQUOTES --label-prefix=inquotes >gensrc/parser2_inquotes.h

clean:
	rm -rf $$(find . \( -name '*.so' -o -name '__pycache__' \) -print) gensrc build .cache

test:	all
	export PYTHONPATH=$$(echo $(CURDIR)/build/lib*); cd tests; python -m pytest -sv test_fastcsv.py test_headers.py test_edge.py test_file.py test_api.py test_chunks.py test_numbers.py test_format.py

benchmark:	all
	export PYTHONPATH=$$(echo $(CURDIR)/build/lib*); cd benchmarks; ./many_doubles.py -n 20000000 --nthreads=4

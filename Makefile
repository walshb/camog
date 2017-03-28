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
	export PYTHONPATH=$$(readlink -f $(CURDIR)/build/lib*); cd tests; python -m pytest --pdb -sv test_fastcsv.py test_headers.py test_edge.py test_format.py test_numbers.py

benchmark:	all
	export PYTHONPATH=$$(readlink -f $(CURDIR)/build/lib*); cd benchmarks; ./many_doubles.py -n 20000000 --nthreads=4

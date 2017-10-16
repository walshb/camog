Camóg
=====

[![Build Status](https://travis-ci.org/walshb/camog.svg?branch=master)](https://travis-ci.org/walshb/camog)

A fast csv parser for Python and R.

```
import camog

headers, columns = camog.load('foobar.csv', nthreads=4)
```

## How should I build it?

```
make benchmark
```

## Benchmarks

Name                                           | Relative speed (4 threads)
-----------------------------------------------|---------------------------
[pandas](https://github.com/pandas-dev/pandas) | 1
[paratext](https://github.com/wiseio/paratext) | 2.5
[camog](https://github.com/walshb/camog)       | 5

## Where's the doc?

```
cat camog/_csv.py
```

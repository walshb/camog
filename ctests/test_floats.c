/*
 * Copyright 2017 Ben Walsh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fastcsv.h"

#ifdef _WIN32
#define PRIx64 "llx"
#else
#include <stdint.h>
#include <inttypes.h>
#endif

typedef struct {
    FastCsvResult r;
    int col_type;
    double result;
} TestFastCsvResult;

static void *
test_add_column(FastCsvResult *res, int col_type, size_t nrows, size_t width)
{
    TestFastCsvResult *testres = (TestFastCsvResult *)res;

    if (col_type != COL_TYPE_DOUBLE && col_type != COL_TYPE_INT64) {
        fprintf(stderr, "Unexpected type %d\n", col_type);
        abort();
    }

    if (nrows != 1) {
        fprintf(stderr, "Unexpected nrows %zd\n", nrows);
        abort();
    }

    testres->col_type = col_type;

    return &testres->result;
}

static int
test_add_header(FastCsvResult *res, const uchar *str, size_t len)
{
    return 0;
}

static int
test_parse_csv(const uchar *csv_buf, size_t buf_len, uchar sep, int nthreads,
               int flags, int nheaders, TestFastCsvResult *result)
{
    FastCsvInput input;

    init_csv(&input, csv_buf, buf_len, nheaders, nthreads);
    input.sep = sep;
    input.flags = flags;

    result->r.add_header = &test_add_header;
    result->r.add_column = &test_add_column;
    result->r.fix_column_type = NULL;

    parse_csv(&input, (FastCsvResult *)result);

    return 0;
}

static int
test_one_float(double v)
{
    double w;
    char buf[256];
    int buf_len;
    TestFastCsvResult result;

    buf_len = sprintf(buf, "%.17g", v);
    w = strtod(buf, NULL);
    if (memcmp(&v, &w, sizeof(double)) != 0) {
        fprintf(stderr, "%s did not round-trip\n", buf);
    }

    test_parse_csv((uchar *)buf, buf_len, ',', 1, 0, 0, &result);

    if (result.col_type == COL_TYPE_DOUBLE
        && memcmp(&v, &result.result, sizeof(double)) != 0) {
        uint64_t vl, wl, rl;
        memcpy(&vl, &v, sizeof(double));
        memcpy(&wl, &w, sizeof(double));
        memcpy(&rl, &result.result, sizeof(double));
        fprintf(stderr, "camog %s => %.17g\n", buf, result.result);
        fprintf(stderr, "original: %" PRIx64 "\n", vl);
        fprintf(stderr, "libc    : %" PRIx64 "\n", wl);
        fprintf(stderr, "camog   : %" PRIx64 "\n", rl);

        fprintf(stderr, "inconsistent!\n");

        exit(1);
    }

    return 0;
}

static int
test_floats(void)
{
    const uint64_t max_counter = ((uint64_t)0x7ff) << 52;
    const uint64_t min_counter = 0;
    uint64_t counter;
    int prev_pct = 0;

    for (counter = min_counter; counter < max_counter; counter += ((1LL << 42) - 1)) {
        double v;
        int pct;

        memcpy(&v, &counter, sizeof(double));

        test_one_float(v);

        pct = (int)(counter >> 48) * 100 / (1 << 15);
        if (pct != prev_pct) {
            fprintf(stderr, "%d%% %.17g\n", pct, v);
            prev_pct = pct;
        }
    }

    return 0;
}

int main(int argc, char *argv[])
{
    if (argc == 2) {
        double v = strtod(argv[1], NULL);
        test_one_float(v);
        return 0;
    }

    test_floats();

    return 0;
}

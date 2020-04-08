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
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "fastcsv.h"

#define BUF_SIZE (8 * 1024 * 1024)

typedef struct {
    size_t len;
    int type;
    int width;
} AflCol;

typedef struct {
    FastCsvResult r;
    void *buf;
    void *buf_last;
    ssize_t nrows;
    int nthreads;
    int n_col_to_type;
    const uchar *col_to_type;
} AflFastCsvResult;

#ifdef PRINT_RESULT
static int
print_result(AflFastCsvResult *res)
{
    int i;
    for (i = 0; i < res->nrows; i++) {
        char *sep = "";
        AflCol *col = (AflCol *)res->buf;
        while ((void *)col < res->buf_last) {
            char *vp = (char *)col + sizeof(AflCol) + i * col->width;
            printf("%s", sep);
            switch (col->type) {
            case COL_TYPE_INT32:
                printf("%" PRId32, *((int32_t *)vp));
                break;
            case COL_TYPE_INT64:
                printf("%" PRId64, *((int64_t *)vp));
                break;
            case COL_TYPE_DOUBLE:
                printf("%f", *((double *)vp));
                break;
            case COL_TYPE_STRING:
                printf("%.*s", (int)col->width, (char *)vp);
                break;
            }
            col = (AflCol *)((char *)col + col->len);
            sep = ",";
        }
        printf("\n");
    }

    return 0;
}
#endif

static void *
afl_add_column(FastCsvResult *res, int col_type, size_t nrows, size_t width)
{
    void *arr;
    size_t len;
    AflFastCsvResult *aflres = (AflFastCsvResult *)res;
    AflCol *col;

    if (aflres->nrows >= 0 && nrows != aflres->nrows) {
        fprintf(stderr, "nrows changed from %zd to %zu\n", aflres->nrows, nrows);
        abort();
    }

    aflres->nrows = nrows;

    switch (col_type) {
    case COL_TYPE_INT32:
        width = sizeof(uint32_t);
        break;
    case COL_TYPE_INT64:
        width = sizeof(uint64_t);
        break;
    case COL_TYPE_DOUBLE:
        width = sizeof(double);
        break;
    case COL_TYPE_STRING:
        break;
    default:
        abort();
    }

    len = sizeof(AflCol) + (nrows * width + sizeof(void *) + 7) & ~7;
    if ((aflres->buf_last - aflres->buf + len) > BUF_SIZE) {
        fprintf(stderr, "Past buf limit.\n");
        exit(1);
    }

    col = (AflCol *)aflres->buf_last;
    aflres->buf_last += len;
    col->len = len;;
    col->type = col_type;
    col->width = width;

    return (char *)col + sizeof(AflCol);
}

static int
afl_add_header(FastCsvResult *res, const uchar *str, size_t len)
{
    return 0;
}

static int
afl_fix_column_type(FastCsvResult *res, int col_idx, int col_type)
{
    AflFastCsvResult *aflres = (AflFastCsvResult *)res;

    if (col_idx < aflres->n_col_to_type) {
        /* return result in range 1-4 */
        return ((int)aflres->col_to_type[col_idx] & 0x3) + 1;
    }

    return col_type;
}

static AflFastCsvResult *
afl_parse_csv(const uchar *csv_buf, size_t buf_len, uchar sep, int nthreads,
              int flags, int nheaders, AflFastCsvResult *result)
{
    FastCsvInput input;

    result->r.add_header = &afl_add_header;
    result->r.add_column = &afl_add_column;
    result->r.fix_column_type = afl_fix_column_type;
    result->buf = malloc(BUF_SIZE);
    result->buf_last = result->buf;
    result->nrows = -1;
    result->nthreads = nthreads;

    result->n_col_to_type = csv_buf[0];
    result->col_to_type = &csv_buf[1];

    if (result->n_col_to_type + 1 > buf_len) {
        return result;
    }

    memset(result->buf, 0, BUF_SIZE);

    csv_buf += result->n_col_to_type + 1;
    buf_len -= result->n_col_to_type + 1;

    init_csv(&input, csv_buf, buf_len, nheaders, nthreads);
    input.sep = sep;
    input.flags = flags;

    parse_csv(&input, (FastCsvResult *)result);

    return result;
}

int main(int argc, char *argv[])
{
    void *filedata;
    int fd;
    struct stat stat_buf;
    AflFastCsvResult result1, result2;
    size_t len1, len2;
    const uchar *data;
    int nthreads;

    if (argc != 2) {
        fprintf(stderr, "Usage: csvread filename\n");
        return 1;
    }

    if ((fd = open(argv[1], O_RDONLY)) < 0) {
        fprintf(stderr, "%s: could not open\n", argv[1]);
        return 1;
    }

    fstat(fd, &stat_buf);

    if (stat_buf.st_size == 0) {
        return 0;
    }

    if ((filedata = mmap(NULL, stat_buf.st_size, PROT_READ, MAP_PRIVATE, fd, 0)) == MAP_FAILED) {
        close(fd);
        fprintf(stderr, "%s: mmap failed\n", argv[1]);
        return 1;
    }

    data = (const uchar *)filedata;

    nthreads = (data[0] & 3) + 2;

    afl_parse_csv(&data[1], stat_buf.st_size - 1, ',', nthreads, 0, 1, &result1);

    afl_parse_csv(&data[1], stat_buf.st_size - 1, ',', 1, 0, 1, &result2);

#ifdef PRINT_RESULT
    print_result(&result1);
#endif

    len1 = result1.buf_last - result1.buf;
    len2 = result2.buf_last - result2.buf;
    if (len1 != len2 || memcmp(result1.buf, result2.buf, len1) != 0) {
        fprintf(stderr, "len1 = %zd len2 = %zd\n", len1, len2);
        fprintf(stderr, "different result with threads\n");
        abort();
    }

    munmap(filedata, stat_buf.st_size);

    close(fd);

    free(result1.buf);
    free(result2.buf);

    return 0;
}

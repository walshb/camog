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

#ifndef _FASTCSV_H
#define _FASTCSV_H

#define COL_TYPE_INT32 1
#define COL_TYPE_INT64 2
#define COL_TYPE_DOUBLE 3
#define COL_TYPE_STRING 4

#define FLAG_EXCEL_QUOTES 1

#define NUMPY_STRING_OBJECT 0

typedef unsigned char uchar;

typedef struct {
    const uchar *csv_buf;
    size_t buf_len;
    uchar sep;
    int nthreads;
    int flags;
    int nheaders;
    int64_t missing_int_val;
    double missing_float_val;
} FastCsvInput;

typedef struct fast_csv_result_s {
    int (*add_header)(struct fast_csv_result_s *, const uchar *, size_t);
    void *(*add_column)(struct fast_csv_result_s *, int, size_t, size_t);
    int (*fix_column_type)(struct fast_csv_result_s *, int, int);
} FastCsvResult;

int init_csv(FastCsvInput *, const uchar *, size_t, int, int);

int parse_csv(const FastCsvInput *, FastCsvResult *);

#endif  /* _FASTCSV_H */

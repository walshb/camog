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

#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

#include "fastcsv.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

typedef struct {
    FastCsvResult r;
    int nrows;
    SEXP headers_cols;
    SEXP last_header;
    SEXP last_col;
} RFastCsvResult;

static int
r_add_header(FastCsvResult *res, const uchar *str, size_t len)
{
    RFastCsvResult *rres = (RFastCsvResult *)res;
    SEXP header_sexp = mkCharLen(str, len);
    SEXP cons = list1(header_sexp);

    if (rres->last_header == NULL) {
        SETCAR(rres->headers_cols, cons);
    } else {
        SETCDR(rres->last_header, cons);
    }
    rres->last_header = cons;

    return 0;
}

static void *
r_add_column(FastCsvResult *res, int col_type, size_t nrows, size_t width)
{
    RFastCsvResult *rres = (RFastCsvResult *)res;
    SEXP vec, cons, nrows_sexp, width_sexp, tuple;
    void *arr;
    int n;

    rres->nrows = nrows;

    switch (col_type) {
    case COL_TYPE_INT32:
    case COL_TYPE_INT64:
        PROTECT(vec = allocVector(INTSXP, nrows));
        arr = INTEGER(vec);
        break;
    case COL_TYPE_DOUBLE:
        PROTECT(vec = allocVector(REALSXP, nrows));
        arr = REAL(vec);
        break;
    case COL_TYPE_STRING:
        n = (nrows * width + sizeof(int) - 1) / sizeof(int);
        PROTECT(vec = allocVector(INTSXP, n));
        arr = INTEGER(vec);
        break;
    }

    PROTECT(nrows_sexp = ScalarInteger((col_type == COL_TYPE_STRING) ? nrows : -1));
    PROTECT(width_sexp = ScalarInteger((col_type == COL_TYPE_STRING) ? width : -1));
    PROTECT(tuple = list3(nrows_sexp, width_sexp, vec));
    PROTECT(cons = list1(tuple));
    if (rres->last_col == NULL) {
        SETCDR(rres->headers_cols, cons);
    } else {
        SETCDR(rres->last_col, cons);
    }
    rres->last_col = cons;

    UNPROTECT(5);

    return arr;
}

static int
r_fix_col_type(FastCsvResult *res, int col_idx, int col_type)
{
    if (col_type == COL_TYPE_INT64) {
        return COL_TYPE_INT32;
    }
    return col_type;
}

static SEXP
convert_to_frame(RFastCsvResult *res)
{
    SEXP frame, cls, names, rownames;
    int col_idx, ncols, header_idx, nheaders, i;
    SEXP cons;
    char rowname[20];

    ncols = length(CDR(res->headers_cols));

    PROTECT(frame = allocVector(VECSXP, ncols));
    col_idx = 0;
    for (cons = CDR(res->headers_cols); cons != R_NilValue; cons = CDR(cons)) {
        int nrows, width;
        SEXP vec;
        SEXP tuple = CAR(cons);
        nrows = INTEGER(CAR(tuple))[0];
        tuple = CDR(tuple);
        width = INTEGER(CAR(tuple))[0];
        tuple = CDR(tuple);
        vec = CAR(tuple);
        if (nrows >= 0 && width >= 0) {
            SEXP str_vec;
            const char *p;
            int i;
            PROTECT(str_vec = allocVector(STRSXP, nrows));
            p = (const char *)INTEGER(vec);
            for (i = 0; i < nrows; i++) {
                SET_STRING_ELT(str_vec, i, mkCharLen(p, strnlen(p, width)));
                p += width;
            }
            SET_VECTOR_ELT(frame, col_idx, str_vec);
            UNPROTECT(1);
        } else {
            SET_VECTOR_ELT(frame, col_idx, vec);
        }
        ++col_idx;
    }

    PROTECT(cls = allocVector(STRSXP, 1));  /* class name */
    SET_STRING_ELT(cls, 0, mkChar("data.frame"));
    classgets(frame, cls);

    nheaders = length(CAR(res->headers_cols));

    PROTECT(names = allocVector(STRSXP, nheaders));  /* column names */
    header_idx = 0;
    for (cons = CAR(res->headers_cols); cons != R_NilValue; cons = CDR(cons)) {
        SET_STRING_ELT(names, header_idx, CAR(cons));
        ++header_idx;
    }
    namesgets(frame, names);

    PROTECT(rownames = allocVector(STRSXP, res->nrows));  /* row names */
    for (i = 0; i < res->nrows; i++) {
        sprintf(rowname, "%d", i);
        SET_STRING_ELT(rownames, i, mkChar(rowname));
    }
    setAttrib(frame, R_RowNamesSymbol, rownames);

    UNPROTECT(4);

    return frame;
}

static SEXP
r_parse_csv(const uchar *csv_buf, size_t buf_len, uchar sep, int nthreads,
            int flags, int nheaders, int64_t missing_int_val, double missing_float_val)
{
    FastCsvInput input;
    RFastCsvResult result;
    SEXP frame;

    init_csv(&input, csv_buf, buf_len, nheaders, nthreads);
    input.sep = sep;
    input.flags = flags;
    input.missing_int_val = missing_int_val;
    input.missing_float_val = missing_float_val;

    result.r.add_header = &r_add_header;
    result.r.add_column = &r_add_column;
    result.r.fix_column_type = &r_fix_col_type;

    PROTECT(result.headers_cols = CONS(R_NilValue, R_NilValue));
    result.last_header = NULL;
    result.last_col = NULL;

    parse_csv(&input, (FastCsvResult *)&result);

    PROTECT(frame = convert_to_frame(&result));

    UNPROTECT(2);

    return frame;
}

static SEXP
csvload(SEXP rfname)
{
    const char *fname;
    SEXP res;
    int fd;
    struct stat stat_buf;
    void *filedata;

    fname = CHAR(STRING_ELT(rfname, 0));

    if ((fd = open(fname, O_RDONLY)) < 0) {
        error("%s: could not open", fname);
    }

    fstat(fd, &stat_buf);

    if ((filedata = mmap(NULL, stat_buf.st_size, PROT_READ, MAP_PRIVATE, fd, 0)) == MAP_FAILED) {
        close(fd);
        error("%s: could not mmap", fname);
    }

    res = r_parse_csv(filedata, stat_buf.st_size, ',', 4, 0, 1,
                      NA_INTEGER, NA_REAL);

    munmap(filedata, stat_buf.st_size);

    close(fd);

    return res;
}

static const R_CMethodDef c_methods[] = {
    {NULL, NULL, 0}
};

static const R_CallMethodDef call_methods[] = {
    {"rccsvload", (DL_FUNC)&csvload, 1},
    {NULL, NULL, 0}
};

void
R_init_camogrc(DllInfo *info)
{
    R_registerRoutines(info, c_methods, call_methods, NULL, NULL);
    R_useDynamicSymbols(info, FALSE);
    R_forceSymbols(info, TRUE);
}

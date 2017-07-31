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

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

#include <fcntl.h>
#include <sys/mman.h>

#include "Python.h"
#include "numpy/arrayobject.h"

#include "fastcsv.h"

typedef struct {
    FastCsvResult r;
    PyObject *headers;
    PyObject *columns;
} PyFastCsvResult;

static void *
py_add_column(FastCsvResult *res, int col_type, size_t nrows, size_t width)
{
    PyObject *arr;
    npy_intp dims[1];
    PyFastCsvResult *pyres = (PyFastCsvResult *)res;

    dims[0] = nrows;

    switch (col_type) {
    case COL_TYPE_INT:
        arr = PyArray_SimpleNew(1, dims, NPY_INT64);
        break;
    case COL_TYPE_DOUBLE:
        arr = PyArray_SimpleNew(1, dims, NPY_FLOAT64);
        break;
    default:
        arr = PyArray_New(&PyArray_Type, 1, dims, NPY_STRING, NULL, NULL, width, 0, NULL);
        break;
    }
    PyList_Append(pyres->columns, arr);  /* increfs */
    Py_DECREF(arr);
    return PyArray_DATA((PyArrayObject *)arr);
}

static int
py_add_header(FastCsvResult *res, const uchar *str, size_t len)
{
    PyObject *str_obj = PyString_FromStringAndSize((char *)str, len);
    PyFastCsvResult *pyres = (PyFastCsvResult *)res;

    PyList_Append(pyres->headers, str_obj);  /* increfs */
    Py_DECREF(str_obj);
    return 0;
}

static PyObject *
py_parse_csv(const uchar *csv_buf, size_t buf_len, PyObject *sep_obj, int nthreads,
             int flags, int nheaders, int64_t missing_int_val, double missing_float_val)
{
    uchar sep;
    FastCsvInput input;
    PyFastCsvResult result;
    PyObject *res_obj;

    if (sep_obj == NULL) {
        sep = ',';
    } else {
        sep = PyString_AsString(sep_obj)[0];
    }

    init_csv(&input, csv_buf, buf_len, nheaders, nthreads);
    input.sep = sep;
    input.flags = flags;
    input.missing_int_val = missing_int_val;
    input.missing_float_val = missing_float_val;

    result.r.add_header = &py_add_header;
    result.r.add_column = &py_add_column;
    if (nheaders == 0) {
        Py_INCREF(Py_None);
        result.headers = Py_None;
    } else {
        result.headers = PyList_New(0);
    }
    result.columns = PyList_New(0);

    parse_csv(&input, (FastCsvResult *)&result);

    res_obj = PyTuple_New(2);
    PyTuple_SET_ITEM(res_obj, 0, result.headers);
    PyTuple_SET_ITEM(res_obj, 1, result.columns);

    return res_obj;
}

static PyObject *
parse_csv_func(PyObject *self, PyObject *args)
{
    PyObject *str_obj, *sep_obj = NULL;
    const uchar *csv_buf;
    size_t buf_len;
    int nthreads = 4;
    int flags = 0;
    int nheaders = 0;
    int missing_int_val = 0;
    double missing_float_val = 0.0;

    if (!PyArg_ParseTuple(args, "O|Oiiiid", &str_obj, &sep_obj, &nthreads,
                          &flags, &nheaders, &missing_int_val, &missing_float_val)) {
        return NULL;
    }

    csv_buf = (uchar *)PyString_AsString(str_obj);
    buf_len = PyString_Size(str_obj);

    return py_parse_csv(csv_buf, buf_len, sep_obj, nthreads, flags, nheaders,
                        missing_int_val, missing_float_val);
}

static PyObject *
parse_file_func(PyObject *self, PyObject *args)
{
    PyObject *fname_obj;
    PyObject *sep_obj = NULL;
    PyObject *res;
    void *filedata;
    const char *fname;
    int nthreads = 4;
    int flags = 0;
    int nheaders = 0;
    int missing_int_val = 0;
    double missing_float_val = 0.0;
    int fd;
    struct stat stat_buf;

    if (!PyArg_ParseTuple(args, "O|Oiiiid", &fname_obj, &sep_obj, &nthreads,
                          &flags, &nheaders, &missing_int_val, &missing_float_val)) {
        return NULL;
    }

    fname = PyString_AsString(fname_obj);
    if ((fd = open(fname, O_RDONLY)) < 0) {
        return PyErr_Format(PyExc_IOError, "%s: could not open", fname);
    }

    fstat(fd, &stat_buf);

    if ((filedata = mmap(NULL, stat_buf.st_size, PROT_READ, MAP_PRIVATE, fd, 0)) == MAP_FAILED) {
        close(fd);
        return PyErr_Format(PyExc_IOError, "%s: mmap failed", fname);
    }

    res = py_parse_csv(filedata, stat_buf.st_size, sep_obj, nthreads, flags, nheaders,
                       missing_int_val, missing_float_val);

    munmap(filedata, stat_buf.st_size);

    close(fd);

    return res;
}

static PyMethodDef mod_methods[] = {
    {"parse_csv", (PyCFunction)parse_csv_func, METH_VARARGS,
     "Parse csv"},
    {"parse_file", (PyCFunction)parse_file_func, METH_VARARGS,
     "Parse csv file"},
    {NULL}  /* Sentinel */
};


#ifndef PyMODINIT_FUNC  /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
init_cfastcsv(void)
{
    PyObject* m;

    import_array();

    m = Py_InitModule3("camog._cfastcsv", mod_methods,
                       "Example module that creates an extension type.");

    (void)m;  /* avoid warning */
}

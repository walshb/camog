import os
import tempfile
import shutil

class TempCsvFile(object):
    def __init__(self, data):
        self._data = data

    def __enter__(self):
        self._dirname = tempfile.mkdtemp()
        fname = os.path.join(self._dirname, 'test.csv')

        with open(fname, 'wb') as fp:
            fp.write(self._data)

        return fname

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self._dirname)

import sys
import time

from contextlib import contextmanager


@contextmanager
def time_elapsed(name=''):
    """
    A context manager for timing blocks of code.
    From https://gist.github.com/raymondbutcher/5168588

    """
    start = time.time()
    yield
    elapsed = (time.time() - start) * 1000
    if name:
        sys.stderr.write('%s took ' % name)
    if elapsed < 1:
        sys.stderr.write('%.4f ms\n' % elapsed)
    else:
        sys.stderr.write('%d ms\n' % elapsed)

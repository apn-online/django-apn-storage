import contextlib
import logging
import os

from fs import _thread_synchronize_default, multifs, tempfs
from fs.base import synchronize
from fs.errors import FSError, OperationFailedError, ResourceNotFoundError


class CreatedDirectory(Exception):
    pass


class MultiFS(multifs.MultiFS):
    """
    This speeds up the original MultiFS class at the expense of accuracy with
    dirs vs files.

    It also allows disabling the default thread locks around all filesystem
    operations, because it seems unnecessary at this level - unless child FS
    objects are frequently updated. Our system initializes the MultiFS and
    then never updates them afterwards.

    It also adds a "test mode" which will automatically clean up any
    written files afterwards.

    """

    def __init__(self, auto_close=False, thread_synchronize=_thread_synchronize_default):

        super(multifs.MultiFS, self).__init__(thread_synchronize=thread_synchronize)

        # By default, don't close the child filesystem objects when closing
        # this object. This is because they might be shared and reused.
        # In particular, make_fs_from_string caches instances and reuses them.
        self.auto_close = auto_close

        self.fs_sequence = []
        self.fs_lookup = {}
        self.write_fs = None

        self.test_mode = False

    @synchronize
    def enable_test_mode(self):
        if not self.test_mode:
            self._test_mode_old_writefs = self.writefs
            self.addfs(
                name='test_mode_fs',
                fs=tempfs.TempFS(),
                write=True,
            )
            self.test_mode = True

    @synchronize
    def disable_test_mode(self):
        if self.test_mode:
            self.writefs.close()
            self.removefs('test_mode_fs')
            self.setwritefs(self._test_mode_old_writefs)
            del self._test_mode_old_writefs
            self.test_mode = False

    @synchronize
    def exists(self, path):
        for fs in self:
            if fs.exists(path):
                return True
        return False

    @synchronize
    def getinfo(self, path):
        for fs in self:
            if fs.exists(path):
                return fs.getinfo(path)
        raise ResourceNotFoundError(path)

    @synchronize
    def getsyspath(self, path, allow_none=False):
        fs = self._delegate_search(path)
        if fs is not None:
            return fs.getsyspath(path, allow_none=allow_none)
        if allow_none and self.writefs is not None:
            return self.writefs.getsyspath(path, allow_none=allow_none)
        raise ResourceNotFoundError(path)

    @synchronize
    def ilistdir(self, path='./', *args, **kwargs):
        seen_paths = set()
        for fs in self:
            try:
                for fs_path in fs.ilistdir(path, *args, **kwargs):
                    if fs_path not in seen_paths:
                        yield fs_path
                        seen_paths.add(fs_path)
            except FSError:
                pass

    @synchronize
    def isdir(self, path):
        for fs in self:
            if fs.isdir(path):
                return True
        return False

    @synchronize
    def isfile(self, path):
        for fs in self:
            if fs.isfile(path):
                return True
        return False

    @synchronize
    def open(self, path, mode='r', *args, **kwargs):
        if not self.fs_sequence:
            logging.warning('%s - fs_sequence is empty' % path)
            logging.warning('%s - closed = %s' % (path, self.closed))
        try:
            with _autocreate_missing_writefs_directory(self, path, mode):
                return super(MultiFS, self).open(path, mode=mode, *args, **kwargs)
        except CreatedDirectory:
            return super(MultiFS, self).open(path, mode=mode, *args, **kwargs)

    @synchronize
    def makedir(self, path, *args, **kwargs):
        if self.writefs is None:
            raise OperationFailedError('makedir', path=path, msg='No writeable FS set')
        return self.writefs.makedir(path, *args, **kwargs)

    @synchronize
    def remove(self, path):
        if self.writefs is None:
            raise OperationFailedError('remove', path=path, msg='No writeable FS set')
        self.writefs.remove(path)

    @synchronize
    def setcontents(self, path, *args, **kwargs):
        if self.writefs is None:
            raise OperationFailedError('setcontents', path=path, msg='No writeable FS set')
        try:
            with _autocreate_missing_writefs_directory(self, path, 'w'):
                return self.writefs.setcontents(path, *args, **kwargs)
        except CreatedDirectory:
            return self.writefs.setcontents(path, *args, **kwargs)


@contextlib.contextmanager
def _autocreate_missing_writefs_directory(fs, path, mode):
    """
    It is possible for a directory to exist on a fallback layer of the multifs,
    but not on the write layer. In this case, write operations can fail.

    This context manager can be used to catch errors from that scenario,
    automatically create the directory on the write layer, and then raise
    the CreatedDirectory exception. The calling code should catch this
    exception and retry the operation.

    Checking for the existence of the directory before attempting write
    operations would be much simpler, but would also be very inefficient.

    """
    try:
        yield
    except (OSError, ResourceNotFoundError):
        if fs.writefs is not None:
            if 'w' in mode or '+' in mode or 'a' in mode:
                # Try to create the directory on the writefs. If this does
                # create the directory, then that was likely the problem,
                # and the operation can be attempted again.
                parent_dir = os.path.dirname(path)
                if not fs.writefs.exists(parent_dir):
                    fs.writefs.makedir(parent_dir, recursive=True, allow_recreate=True)
                    raise CreatedDirectory
        # It wasn't the above situation, so propagate the error.
        raise

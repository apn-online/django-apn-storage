import functools
import os

from fs import filelike, tempfs, wrapfs
from fs.errors import ResourceNotFoundError
from fs.path import normpath


class CacheFS(wrapfs.WrapFS):

    def __init__(self, fs, cachefs):
        self.cachefs = cachefs
        self.test_mode = False
        super(CacheFS, self).__init__(fs)

    def __del__(self):
        # Note: the new version of the fs library does this to avoid errors.
        if not getattr(self, 'closed', True) and hasattr(self, 'close'):
            try:
                self.close()
            except:
                pass

    def _purge(self, path):
        try:
            self.cachefs.remove(path)
        except ResourceNotFoundError:
            pass

    def enable_test_mode(self):
        if not self.test_mode:
            self._cachefs = self.cachefs
            self.cachefs = tempfs.TempFS()
            self.test_mode = True

    def disable_test_mode(self):
        if self.test_mode:
            self.cachefs = self._cachefs
            self.test_mode = False

    def open(self, path, mode='r', *args, **kwargs):

        path = normpath(path)

        if 'w' in mode or 'a' in mode or '+' in mode:
            # This file is being opened in a write mode, so bypass the cache.
            # Wrap the file object so it removes the file from the cache
            # when it is closed, ensuring that future read operations will
            # access the underlying filesystem and get the latest version.
            open_file = super(CacheFS, self).open(path, mode=mode, *args, **kwargs)
            purge_path = functools.partial(self._purge, path=path)
            return CacheFile(
                open_file=open_file,
                mode=mode,
                on_close=purge_path,
            )

        try:
            return self.cachefs.open(path, mode=mode, *args, **kwargs)
        except ResourceNotFoundError:

            # Open the original file in binary mode
            # to ensure the file contents are copied properly.
            old_file = super(CacheFS, self).open(path, mode='rb')

            # Copy it onto the cache.
            try:
                new_file = self.cachefs.open(path, 'wb')
            except ResourceNotFoundError:
                self.cachefs.makedir(
                    path=os.path.dirname(path),
                    recursive=True,
                    allow_recreate=True,
                )
                new_file = self.cachefs.open(path, 'wb')
            old_file.seek(0)
            new_file.write(old_file.read())
            new_file.close()

            # Now return a file. Try to reuse the existing file object if
            # it was opened with the desired mode. Otherwise, reopen it.
            if mode == 'rb':
                old_file.seek(0)
                return old_file
            else:
                old_file.close()
                new_file = self.cachefs.open(path, mode=mode, *args, **kwargs)
                return new_file

    def setcontents(self, path, *args, **kwargs):
        path = normpath(path)
        try:
            return super(CacheFS, self).setcontents(path, *args, **kwargs)
        finally:
            self._purge(path)


class CacheFile(filelike.FileWrapper):

    def __init__(self, open_file, mode, on_close):
        super(CacheFile, self).__init__(open_file, mode)
        self.on_close = on_close

    def close(self):
        try:
            return super(CacheFile, self).close()
        finally:
            self.on_close()


enable_caching = CacheFS

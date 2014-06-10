"""
Copied from pyfilesystem trunk and modified to suit.

https://code.google.com/p/pyfilesystem/source/browse/trunk/fs/wrapfs/hidefs.py

"""

import fnmatch
import re

from fs.errors import ResourceNotFoundError
from fs.path import iteratepath, normpath, pathjoin
from fs.wrapfs import rewrite_errors, WrapFS

from apn_storage.utils import pathcombine


class HideFS(WrapFS):
    """
    FS wrapper that hides resources if they match a wildcard(s).

    For example, to hide all pyc file and subversion directories
    from a filesystem::

        hide_fs = HideFS(my_fs, '*.pyc', '.svn')

    """

    def __init__(self, wrapped_fs, *hide_wildcards):
        self._hide_wildcards = [re.compile(fnmatch.translate(wildcard)) for wildcard in hide_wildcards]
        super(HideFS, self).__init__(wrapped_fs)

    def __del__(self):
        # Note: the new version of the fs library does this to avoid errors.
        if not getattr(self, 'closed', True) and hasattr(self, 'close'):
            try:
                self.close()
            except Exception:
                pass

    def _should_hide(self, path):
        return any(any(wildcard.match(part) for wildcard in self._hide_wildcards) for part in iteratepath(path))

    def _should_show(self, path):
        return not self._should_hide(path)

    def _encode(self, path):
        path = normpath(path)
        if self._should_hide(path):
            raise ResourceNotFoundError(path)
        return path

    def _decode(self, path):
        return path

    def exists(self, path):
        path = normpath(path)
        if self._should_hide(path):
            return False
        return super(HideFS, self).exists(path)

    def listdir(self, path='./', **kwargs):
        path = normpath(path)
        entries = super(HideFS, self).listdir(path, **kwargs)
        if kwargs.get('full'):
            should_show = lambda entry: self._should_show(entry)
        elif kwargs.get('absolute'):
            should_show = lambda entry: self._should_show(entry.lstrip('/'))
        else:
            should_show = lambda entry: self._should_show(pathcombine(path, entry))
        entries = [entry for entry in entries if should_show(entry)]
        return entries

    def ilistdir(self, path='./', **kwargs):
        path = normpath(path)
        if kwargs.get('full'):
            should_show = lambda entry: self._should_show(entry)
        elif kwargs.get('absolute'):
            should_show = lambda entry: self._should_show(entry.lstrip('/'))
        else:
            should_show = lambda entry: self._should_show(pathcombine(path, entry))
        for entry in super(HideFS, self).ilistdir(path, **kwargs):
            if should_show(entry):
                yield path

    def ilistdirinfo(self, path='./', **kwargs):
        path = normpath(path)
        if kwargs.get('full'):
            should_show = lambda entry: self._should_show(entry)
        elif kwargs.get('absolute'):
            should_show = lambda entry: self._should_show(entry.lstrip('/'))
        else:
            should_show = lambda entry: self._should_show(pathcombine(path, entry))
        for entry in super(HideFS, self).ilistdirinfo(path, **kwargs):
            if should_show(entry[0]):
                yield entry

    @rewrite_errors
    def walk(self, path='/', wildcard=None, dir_wildcard=None, search='breadth', ignore_errors=False):
        # Note: copy/pasted this to avoid walking into hidden directories.

        path = normpath(path)

        def listdir(path, *args, **kwargs):
            try:
                return self.listdir(path, *args, **kwargs)
            except Exception:
                if ignore_errors:
                    return []
                else:
                    raise

        if wildcard is None:
            wildcard = lambda f: True
        elif not callable(wildcard):
            wildcard_re = re.compile(fnmatch.translate(wildcard))
            wildcard = lambda fn: bool(wildcard_re.match(fn))

        if dir_wildcard is None:
            dir_wildcard = lambda f: True
        elif not callable(dir_wildcard):
            dir_wildcard_re = re.compile(fnmatch.translate(dir_wildcard))
            dir_wildcard = lambda fn: bool(dir_wildcard_re.match(fn))

        if search == 'breadth':

            dirs = [path]
            while dirs:
                current_path = dirs.pop()
                paths = []
                try:
                    for filename in listdir(current_path):
                        path = pathjoin(current_path, filename)
                        if self.isdir(path):
                            if dir_wildcard(path) and self._should_show(path):
                                dirs.append(path)
                        else:
                            if wildcard(filename) and self._should_show(path):
                                paths.append(filename)
                except ResourceNotFoundError:
                    # Could happen if another thread / process deletes something whilst we are walking
                    pass

                yield (current_path, paths)

        elif search == 'depth':

            def recurse(recurse_path):
                try:
                    for path in listdir(recurse_path, wildcard=dir_wildcard, full=True, dirs_only=True):
                        for p in recurse(path):
                            yield p
                except ResourceNotFoundError:
                    # Could happen if another thread / process deletes something whilst we are walking
                    pass

                filenames = listdir(recurse_path, wildcard=wildcard, files_only=True)
                filenames = [filename for filename in filenames if self._should_show(pathcombine(recurse_path, filename))]
                yield (recurse_path, filenames)

            for p in recurse(path):
                yield p

        else:
            raise ValueError("Search should be 'breadth' or 'depth'")

    def walkfiles(self, path='/', wildcard=None, dir_wildcard=None, search='breadth', ignore_errors=False):
        # Bypass the WrapFS optimization because it avoids using
        # the "walk" method from this class.
        items = self.walk(path, wildcard=wildcard, dir_wildcard=dir_wildcard, search=search, ignore_errors=ignore_errors)
        for path, filenames in items:
            for filename in filenames:
                yield pathjoin(path, filename)

    def walkdirs(self, path='/', wildcard=None, search='breadth', ignore_errors=False):
        # Bypass the WrapFS optimization because it avoids using
        # the "walk" method from this class.
        items = self.walk(path, dir_wildcard=wildcard, search=search, ignore_errors=ignore_errors)
        for path, _filenames in items:
            yield path


class HidePathsFS(HideFS):
    """
    FS wrapper that hides entire paths if they match a wildcard(s).

    For example, to hide all "temporary" directories from a filesystem::

        hide_fs = HidePathsFS(my_fs, 'tmp', 'media/tmp')

    Note: Do not prefix paths with a slash.

    """

    def _should_hide(self, path):
        path = path.lstrip('/')
        return any(wildcard.match(path) for wildcard in self._hide_wildcards)


hide_filenames = HideFS
hide_paths = HidePathsFS

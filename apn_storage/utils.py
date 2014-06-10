import codecs
import collections
import datetime
import functools
import os
import threading

from fs import osfs, tempfs, utils
from fs.errors import ParentDirectoryMissingError, ResourceNotFoundError

from apn_storage import httpfs, s3fs


def thread_locked(func):
    """
    Decorate a function so that it can only be run
    by one thread at a time within a process.

    """

    lock = threading.Lock()

    @functools.wraps(func)
    def locked_func(*args, **kwargs):
        with lock:
            return func(*args, **kwargs)

    return locked_func


@thread_locked
def make_fs_from_string(string, _cache={}):
    """
    Create a FS object from a string. Uses a cache to avoid creating multiple
    FS objects for any given string (except for tempfs which allows multple
    instances).

    """

    if string == 'tempfs':
        # Use a temporary filesystem which is only available to the current
        # process, and will be cleaned up automatically. Bypass the cache
        # for this type of filesystem, as they are unique and self-contained.
        return tempfs.TempFS()

    if string.startswith('~/'):
        string = os.path.expanduser(string)

    if string in _cache:
        return _cache[string]

    if string.startswith('/'):

        # Use a simple directory on the filesystem.
        if not os.path.exists(string):
            osfs.OSFS('/', dir_mode=0775).makedir(
                path=string,
                recursive=True,
                allow_recreate=True,
            )
        fs = osfs.OSFS(string, dir_mode=0775)

    elif string.startswith('s3:'):

        # Use an S3 bucket.
        s3_bucket = string[3:]

        if '/' in s3_bucket:
            s3_bucket, path = s3_bucket.split('/', 1)
        else:
            path = ''

        # The S3FS class can poll S3 for a file's etag after writing
        # to it, to ensure that the file upload has been written to
        # all relevant nodes in the S3 cluster.

        # S3 has read-after-write consistency for PUTS of new objects
        # and eventual consistency for overwrite PUTS and DELETES.
        # See http://aws.amazon.com/s3/faqs/

        # Most of our operations are writing to new files, so disable
        # this mostly wasteful check. This might need to be revisited
        # if there is a special case where we're updating files.
        key_sync_timeout = None

        fs = s3fs.S3FS(s3_bucket, key_sync_timeout=key_sync_timeout)

        if path:
            fs = fs.makeopendir(path, recursive=True)

    elif string.startswith('http://'):

        fs = httpfs.HTTPFS(string)

    else:
        raise ValueError('Unsupported storage string %r' % string)

    _cache[string] = fs
    return fs


def pathcombine(path1, path2):
    """
    Note: This is copied from:
    https://code.google.com/p/pyfilesystem/source/browse/trunk/fs/path.py

    Joins two paths together.

    This is faster than `pathjoin`, but only works when the second path is relative,
    and there are no backreferences in either path.

    >>> pathcombine("foo/bar", "baz")
    'foo/bar/baz'

    """
    if not path1:
        return path2.lstrip()
    return '%s/%s' % (path1.rstrip('/'), path2.lstrip('/'))


def find_old_files(fs, timedelta):
    """Find all files with a modified time older than the timedelta."""
    for path in fs.walkfiles():
        info = fs.getinfo(path)
        modified = info['modified_time']
        age = datetime.datetime.now() - modified
        if age > timedelta:
            yield path


def cleanup_old_files(fs, timedelta, *paths):
    """Delete files with a modified time older than the timedelta."""
    removed = []
    for root_path in paths:
        try:
            path_fs = fs.opendir(root_path)
        except ResourceNotFoundError:
            pass
        else:
            for path in find_old_files(path_fs, timedelta):
                try:
                    path_fs.remove(path)
                except ResourceNotFoundError:
                    pass
                else:
                    full_path = pathcombine(root_path, path)
                    removed.append(full_path)
    return removed


def copy_file(source_fs, source_path, target_fs, target_path, overwrite=True, create_directory=True, chunk_size=64 * 1024):
    """
    Copy a file from one filesystem/path to another filesystem/path.
    This will create the target directory if necessary,
    unless create_directory is set to False.

    """

    try:
        utils.copyfile(source_fs, source_path, target_fs, target_path, overwrite=overwrite, chunk_size=chunk_size)
    except ParentDirectoryMissingError:
        if create_directory:
            target_fs.makedir(os.path.dirname(target_path), recursive=True, allow_recreate=True)
            utils.copyfile(source_fs, source_path, target_fs, target_path, overwrite=overwrite, chunk_size=chunk_size)
        else:
            raise


def move_file(fs, current_path, target_path, overwrite=True):

    try:
        fs.move(current_path, target_path, overwrite=overwrite)
    except ParentDirectoryMissingError:
        parent_directory = os.path.dirname(target_path)
        fs.makedir(parent_directory, recursive=True, allow_recreate=True)
        fs.move(current_path, target_path, overwrite=overwrite)


def use_codec(open_file, encoding=None, errors='strict'):
    """
    This is the same as "codecs.open()" but it uses
    an already open file instead of a file path to open.

    """
    if encoding is None:
        return open_file
    info = codecs.lookup(encoding)
    srw = codecs.StreamReaderWriter(open_file, info.streamreader, info.streamwriter, errors)
    # Add attributes to simplify introspection
    srw.encoding = encoding
    return srw


WalkNode = collections.namedtuple('WalkNode', ('path', 'isdir', 'size'))


def walk_fs(fs, path='/', sort=True, max_depth=float('inf')):

    nodes = []

    for (path, info) in fs.ilistdirinfo(path, full=True):
        node = WalkNode(
            path=path,
            isdir=fs.isdir(path),
            size=info.get('size', 0),
        )
        nodes.append(node)

    if sort:
        nodes.sort()

    for node in nodes:
        yield node
        if node.isdir:
            if max_depth > 0:
                children = walk_fs(
                    fs=fs,
                    path=node.path,
                    sort=sort,
                    max_depth=max_depth - 1,
                )
                for child_node in children:
                    yield child_node


def walk_files(fs, path='/', sort=True, max_depth=float('inf')):
    for node in walk_fs(fs, path=path, sort=sort, max_depth=max_depth):
        if not node.isdir:
            yield node

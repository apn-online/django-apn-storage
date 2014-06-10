import os

from boto.s3.prefix import Prefix

from fs import s3fs
from fs.errors import ResourceInvalidError, ResourceNotFoundError
from fs.wrapfs.subfs import SubFS

from lazyobject import ThreadSafeLazyObject


class S3FS(s3fs.S3FS):
    """
    Extended to support boto's preferred way of reading credentials
    from a configuration file. Also, there is an ilistdir() optimization
    that allows checking isdir() and isfile() without hitting S3 again.

    """

    def __init__(self, *args, **kwargs):

        # Use some dummy values to avoid the validation in the init method.
        kwargs.update({'aws_access_key': '', 'aws_secret_key': ''})

        super(S3FS, self).__init__(*args, **kwargs)

        # Remove the access keys created by the init method. They are passed
        # into the S3Connection as *args so making an empty tuple does nothing.
        self._access_keys = ()

    @property
    def _is_dir_dict(self):
        # Use threadlocals to remember whether a path is a directory or file.
        # This is populated during _iter_keys and then cleared when it exits.
        if not hasattr(self._tlocal, 'is_dir_dict'):
            setattr(self._tlocal, 'is_dir_dict', {})
        return getattr(self._tlocal, 'is_dir_dict')

    def _iter_keys(self, path):
        """
        Iterator over keys contained in the given directory.

        This generator yields (name, key) pairs for each entry in the given
        directory.  If the path is not a directory, it raises the approprate
        error.

        This has been overriden to update the is_dir_dict cache during
        iteration of a directory. This lets other methods access this extra
        information without having to make more API calls. The cache is
        cleared after the iteration completes, so it won't become stale.

        """

        s3path = self._s3path(path) + self._separator
        if s3path == "/":
            s3path = ""

        isDir = False

        is_dir_dict = self._is_dir_dict
        try:
            for k in self._s3bukt.list(prefix=s3path, delimiter=self._separator):

                if not isDir:
                    isDir = True

                # Skip over the entry for the directory itself, if it exists
                name = self._uns3path(k.name, s3path)
                if name != "":

                    if not isinstance(name, unicode):
                        name = name.decode("utf8")
                    if name.endswith(self._separator):
                        name = name[:-1]

                    # Record whether this path is a directory or not.
                    is_dir_dict[os.path.join(path, name)] = isinstance(k, Prefix)

                    yield (name, k)
        finally:
            # Always clear this afterwards.
            # The idea is to check the values while iterating.
            is_dir_dict.clear()

        if not isDir:
            if s3path != self._prefix:
                if self.isfile(path):
                    msg = "that's not a directory: %(path)s"
                    raise ResourceInvalidError(path, msg=msg)
                raise ResourceNotFoundError(path)

    def copy(self, *args, **kwargs):
        # Override this because it raises the wrong exception
        # when the source file does not exist.
        try:
            return super(S3FS, self).copy(*args, **kwargs)
        except ResourceInvalidError as error:
            raise ResourceNotFoundError(error.path)

    def isdir(self, path):
        is_dir_dict = self._is_dir_dict
        if path in is_dir_dict:
            return is_dir_dict[path]
        else:
            return super(S3FS, self).isdir(path)

    def isfile(self, path):
        is_dir_dict = self._is_dir_dict
        if path in is_dir_dict:
            return not is_dir_dict[path]
        else:
            return super(S3FS, self).isfile(path)

    def opendir(self, path):
        return self._sub_fs(path)

    def makeopendir(self, path, recursive=False):
        return self._sub_fs(path, make=True, make_recursive=recursive)

    def _sub_fs(self, path, make=False, make_recursive=False):
        """
        Create a lazy SubFS for the given path. This avoids
        performing operations on S3 until it needs to to be used.

        """

        fs = self

        class LazySubS3FS(ThreadSafeLazyObject):
            def _setup(self):
                if make:
                    fs.makedir(path, allow_recreate=True, recursive=make_recursive)
                elif not fs.exists(path):
                    raise ResourceNotFoundError(path)
                self._wrapped = SubFS(fs, path)

        return LazySubS3FS()

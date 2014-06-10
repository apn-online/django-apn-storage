import os
import re

from django.core.files import File
from django.template.defaultfilters import slugify

from fs.errors import convert_fs_errors, ResourceNotFoundError
from fs.expose import django_storage
from fs.path import dirname

from apn_storage.contrib import uid


class FSStorage(django_storage.FSStorage):
    """
    A storage class for Django's file storage API.
    Used by file fields, etc.

    """

    def __init__(self, fs, base_url):

        self.fs = fs
        self.base_url = base_url.rstrip('/')

        # When saving files, it will use a UID generator to append unique
        # strings at the end of the filenames. This ensures unique names
        # and avoids race conditions. Create a regular expression here
        # for matching those UID suffixes.
        self._uid_generator = uid.alphanumeric_lowercase
        self._uid_suffix_re = re.compile('-[%s]{%d}$' % (
            self._uid_generator.alphabet,
            self._uid_generator.length,
        ))

    @convert_fs_errors
    def _open(self, name, mode):
        fs_file = self.fs.open(name, mode)
        return FSStorageFile(fs_file, storage=self, name=name)

    @convert_fs_errors
    def _save(self, name, content):
        self.fs.makedir(dirname(name), allow_recreate=True, recursive=True)
        if hasattr(content, 'seek'):
            content.seek(0)
        self.fs.setcontents(name, content)
        return name

    @convert_fs_errors
    def delete(self, name):
        """Override this because the parent class has a missing import."""
        try:
            self.fs.remove(name)
        except ResourceNotFoundError:
            pass

    def exists(self, name):
        """
        Override this because it was previously checking "isfile",
        but Django's Storage API normally returns True for directories too.

        """
        return self.fs.exists(name)

    def get_available_name(self, name):
        """
        Generates a unique name which is practically guaranteed to not exist
        already. This does not need to check if the file exists, because the
        generated name uses a UID (which is pretty close to guaranteed to
        being unique). Also slugifies the name to make it more web-friendly.

        Example:
            from: media/images/My Test.jpg
              to: media/images/my-test-ggn4qyjtl4c87v6u8h2.jpg

        """

        directory, filename = os.path.split(name)
        filename, extension = os.path.splitext(filename)

        # Retain the second extension for .gz files.
        if extension.lower() == '.gz':
            filename, extension = os.path.splitext(filename)
            extension += '.gz'

        # Remove existing UUID suffixes from filenames.
        filename = self._uid_suffix_re.sub('', filename)

        # Clean up the filename and append a UUID to it.
        filename = '%s-%s' % (
            slugify(filename).encode('ascii'),
            self._uid_generator(),
        )

        return os.path.join(directory, filename + extension)

    def get_available_name_regex(self, name):

        directory, filename = os.path.split(name)
        filename, extension = os.path.splitext(filename)

        # Retain the second extension for .gz files.
        if extension.lower() == '.gz':
            filename, extension = os.path.splitext(filename)
            extension += '.gz'

        # Remove existing UUID suffixes from filenames.
        filename = self._uid_suffix_re.sub('', filename)

        # Clean up the filename and append a UUID to it.
        filename = '%s-%s' % (
            slugify(filename).encode('ascii'),
            '[%s]{%d}' % (self._uid_generator.alphabet, self._uid_generator.length)
        )

        return os.path.join(directory, filename + extension)

    @convert_fs_errors
    def listdir(self, path):
        """Use ilistdir to optimize the isdir checks."""
        directories, files = [], []
        for entry in self.fs.ilistdir(path):
            if self.fs.isdir(os.path.join(path, entry)):
                directories.append(entry)
            else:
                files.append(entry)
        return directories, files

    def path(self, name):
        """The file may not be stored locally, so this is not supported."""
        raise NotImplementedError


class FSStorageFile(File):

    def __init__(self, file, storage, name):
        self.file = file
        self.storage = storage
        self.name = name
        self.mode = getattr(file, 'mode', None)

    def _get_size(self):
        if not hasattr(self, '_size'):
            self._size = self.storage.size(self.name)
        return self._size

    def _set_size(self, size):
        self._size = size

    size = property(_get_size, _set_size)

    def open(self, mode=None):
        if not self.closed:
            self.seek(0)
        elif self.storage.exists(self.name):
            self.file = self.storage.open(self.name, self.mode)
        else:
            raise ValueError("The file cannot be reopened.")

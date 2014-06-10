import contextlib
import os
import threading
import uuid

from django.conf import settings
from django.core.files.base import ContentFile
from django.test import TestCase

from fs.errors import ResourceNotFoundError

from apn_storage import layeredfs, s3fs
from apn_storage.contrib import uid
from apn_storage.contrib.time_elapsed import time_elapsed
from apn_storage.django_storage import FSStorage
from apn_storage.utils import make_fs_from_string
from apn_storage.wrapfs import cachefs


class UIDTestCase(TestCase):

    def assertUnique(self, generate_function, loops=1000, threads=5):

        generated = set()

        def generate_loop():
            for x in xrange(loops):
                generated.add(generate_function())

        generate_threads = [threading.Thread(target=generate_loop) for x in xrange(threads)]
        [thread.start() for thread in generate_threads]
        [thread.join() for thread in generate_threads]

        self.assertEqual(len(generated), loops * threads)

    def test_uid_alphanumeric(self):
        self.assertUnique(uid.alphanumeric)

    def test_uid_alphanumeric_lowercase(self):
        self.assertUnique(uid.alphanumeric_lowercase)

    def test_uid_junk(self):
        # Test with an alphabet of only 2 characters.
        # This makes longer strings but it still works.
        junk = uid.UIDGenerator('ab')
        self.assertUnique(junk)

    def disabled_test_uuid(self):
        # I've seen reports/questions of uuid1's uniqueness, but it seems OK.
        # The output is longer than uid's functions so I'll stick with uid.
        uuid1_hex = lambda: uuid.uuid1().hex
        self.assertUnique(uuid1_hex, loops=10000)


class DjangoStorageTests(TestCase):
    """
    This is the base class for testing storage objects.
    It gets deleted at the end of the module to avoid being tested itself.

    """

    storage = None

    # Use a unique test directory to avoid issues when multiple
    # machines run tests using the same S3 bucket at the same time.
    test_dir = '/test/%s' % uid.alphanumeric()

    cats = os.path.join(test_dir, 'cats.txt')
    cats1 = os.path.join(test_dir, 'cats_1.txt')

    dogs = os.path.join(test_dir, 'subdir', 'dogs.txt')
    dogs1 = os.path.join(test_dir, 'subdir', 'dogs_1.txt')

    created_dirs = False

    def setUp(self):

        if hasattr(self.storage.fs, 'fs_sequence'):
            fs_list = self.storage.fs.fs_sequence
        else:
            fs_list = [self.storage.fs]

        for fs in fs_list:

            # Trigger a once-off request now, to prevent it from running
            # within the the individual tests and affecting their times.
            if isinstance(fs, s3fs.S3FS):
                with timed('initialize s3 bucket'):
                    fs._s3bukt

            # Clean up the test environment.
            with timed('remove x4'):
                for path in (self.cats, self.cats1, self.dogs, self.dogs1):
                    try:
                        fs.remove(path)
                    except ResourceNotFoundError:
                        pass

        # Ensure the directory exists so it can be written to.
        if not self.__class__.created_dirs:
            with timed('makedir x4'):
                self.storage.fs.makedir(
                    os.path.dirname(self.dogs),
                    recursive=True,
                    allow_recreate=True,
                )
            self.__class__.created_dirs = True

    def test_delete(self):

        # Roughly 1 seconds for a delete, because it checks that it's not
        # a directory, and it also polls until the file is properly deleted.
        # The polling seems unnecessary, we could possible disable that.

        with timed('setcontents'):
            self.storage.fs.setcontents(self.cats, 'cats on s3')

        with timed('exists'):
            self.assertTrue(self.storage.exists(self.cats))

        with timed('delete'):
            self.storage.delete(self.cats)

        with timed('exists'):
            self.assertFalse(self.storage.exists(self.cats))

    def test_exists(self):

        with timed('setcontents'):
            self.storage.fs.setcontents(self.cats, 'cats on s3')

        with timed('exists'):
            self.assertTrue(self.storage.exists(self.cats))

        # Also test that "exists" works with directories.
        with timed('listdir'):
            directories, filenames = self.storage.listdir('')
        self.assertTrue(directories)
        with timed('exists'):
            self.assertTrue(self.storage.exists(directories[0]))

    def test_get_available_name(self):

        original_name = 'media/images/My Test.jpg'

        available_name = self.storage.get_available_name(original_name)
        self.assertNotEqual(available_name, original_name)
        self.assertFalse(' ' in available_name)

        next_name = self.storage.get_available_name(available_name)
        self.assertNotEqual(next_name, available_name)
        self.assertEqual(len(next_name), len(available_name))

    def test_get_valid_name(self):

        name = os.path.basename(self.cats)

        with timed('get_valid_name'):
            self.assertEqual(
                self.storage.get_valid_name(name),
                name,
            )

    def test_listdir(self):

        with timed('setcontents x2'):
            self.storage.fs.setcontents(self.cats, 'cats on s3')
            self.storage.fs.setcontents(self.dogs, 'dogs on s3')

        with timed('listdir'):
            dirs, files = self.storage.listdir(os.path.dirname(self.cats))

        self.assertTrue(os.path.basename(self.cats) in files)
        self.assertTrue(os.path.basename(os.path.dirname(self.dogs)) in dirs)

    def test_open(self):

        written_content = 'cats on s3, %s\n' % uid.alphanumeric()

        with timed('open/write'):
            cat = self.storage.open(self.cats, 'w')
            cat.write(written_content)
            cat.close()

        with timed('open/append'):
            cat = self.storage.open(self.cats, 'a')
            cat.write(written_content)
            cat.close()

        with timed('open/read'):
            cat = self.storage.open(self.cats, 'r')
            fetched_content = cat.read()
            cat.close()

        self.assertEqual(fetched_content, written_content + written_content)

    def test_path(self):
        try:
            self.storage.path(self.cats)
        except NotImplementedError:
            pass
        else:
            self.fail('The path method should raise NotImplementedError.')

    def test_save(self):

        # Clean up any existing files.
        with timed('delete x2'):
            self.storage.delete(self.cats)
            self.storage.delete(self.cats1)
        with timed('exists x2'):
            self.assertFalse(self.storage.exists(self.cats))
            self.assertFalse(self.storage.exists(self.cats1))

        content = 'cats on s3, %s\n' % uid.alphanumeric()

        # Use the save method and ensure it worked.
        with timed('save'):
            saved_path = self.storage.save(self.cats, ContentFile(content))
        try:
            self.assertNotEqual(saved_path, self.cats)
            with timed('getcontents'):
                self.assertEqual(
                    self.storage.fs.getcontents(saved_path),
                    content,
                )
        finally:
            with timed('delete'):
                self.storage.delete(saved_path)

        # But the 2nd path should not exist yet.
        with timed('exists'):
            self.assertFalse(self.storage.exists(self.cats1), self.cats1)

        # Now save the file again, and ensure it worked, and saved
        # to the 2nd path according to the "get_available_name" feature.
        with timed('save'):
            saved_path = self.storage.save(self.cats, ContentFile(content))
        try:
            self.assertNotEqual(saved_path, self.cats1)
            with timed('getcontents'):
                self.assertEqual(
                    self.storage.fs.getcontents(saved_path),
                    content,
                )
        finally:
            with timed('delete'):
                self.storage.delete(saved_path)

    def test_size(self):

        content = 'cats on s3, %s\n' % uid.alphanumeric()

        with timed('setcontents'):
            self.storage.fs.setcontents(self.cats, content)

        with timed('size'):
            self.assertEqual(
                self.storage.size(self.cats),
                len(content),
            )

    def test_url(self):
        expected_url = os.path.join(
            settings.MEDIA_STORAGE_URL.rstrip('/'),
            self.cats.lstrip('/'),
        )
        self.assertEqual(
            self.storage.url(self.cats),
            expected_url,
        )


class SingleServerStorageTests(DjangoStorageTests):
    """
    Test a configuration for localdev / integration / jenkins.

    These environments each only have a single server,
    so all file operations can be performed on a single disk.

    It uses an S3 bucket as a fallback for reading files.
    This allows the local disk to have only deltas.

    """

    storage = FSStorage(
        fs=layeredfs.make_layered_fs(
            settings.MEDIA_STORAGE_1,
            's3:apn-localdev-test1',
        ),
        base_url=settings.MEDIA_STORAGE_URL,
    )


class StagingServerStorageTests(DjangoStorageTests):
    """
    Test a configuration for staging environments.

    These environments each involve multiple servers, so they cannot write
    files to the local disk; the other servers could not access them.

    To minimize storage usage, it uses one S3 bucket for reads and writes,
    with another S3 bucket as a fallback for reading files. This allows
    the first bucket to only have deltas.

    """

    storage = FSStorage(
        fs=layeredfs.make_layered_fs(
            's3:apn-localdev-test1',
            's3:apn-localdev-test2',
        ),
        base_url=settings.MEDIA_STORAGE_URL,
    )


class ProductionServerStorageTests(DjangoStorageTests):
    """
    Test a configuration for the production environment.

    This environment involves multiple servers, so they cannot write
    files to the local disk; the other servers could not access them.

    This is a simple, single S3 bucket. This is our master repository of
    files, used by all other enviroments as a fallback for reading files.

    """

    storage = FSStorage(
        fs=layeredfs.make_layered_fs(
            's3:apn-localdev-test1',
        ),
        base_url=settings.MEDIA_STORAGE_URL,
    )


class UnicodeTests(TestCase):

    def make_storage(self):
        """
        Create a storage object that will exercise
        multiple FS types and wrapping layers.

        """

        fs = layeredfs.make_layered_fs(
            '/tmp',
            's3:apn-localdev-test1/test',
            'http://media2.apnonline.com.au/img/'
        )
        fs = cachefs.enable_caching(fs, cachefs=make_fs_from_string('tempfs'))

        return FSStorage(fs, base_url=settings.MEDIA_STORAGE_URL)

    def setUp(self):
        self.storage = self.make_storage()

    def test_utf8(self):

        # A fake path with a non-ascii character in it.
        badpath_unicode = unicode('dir/file\xc2\xacname_%s.jpg' % uid.alphanumeric(), 'utf-8')
        badpath_utf8 = badpath_unicode.encode('utf-8')

        # UTF-8 encoded strings with non-ascii characters are not supported
        # in the fs library. I'm asserting this behaviour because it is the
        # current known behavior, not because it is the desired behavior.
        # This test is here to let people know if the situation ever changes.
        # Note: it seems this issue only exists when there is a combination
        # of s3fs and subfs.
        try:
            self.storage.exists(badpath_utf8)
        except UnicodeDecodeError:
            pass
        else:
            self.fail('The storage object seems to work with utf-8 strings now???')

        # Unicode strings containing weird characters should work.
        self.storage.exists(badpath_unicode)


@contextlib.contextmanager
def timed(*args):
    disabled = True
    if disabled:
        yield
    else:
        print '-' * 80
        with time_elapsed(*args):
            yield
        print '-' * 80


# Don't test the base class.
del DjangoStorageTests

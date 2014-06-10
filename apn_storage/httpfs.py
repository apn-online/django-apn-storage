import datetime
import logging
import requests
import time

from django.utils.http import urlquote

from email.Utils import parsedate

from fs.base import FS
from fs.errors import RemoteConnectionError, ResourceNotFoundError, UnsupportedError
from fs.filelike import StringIO


class HTTPFS(FS):

    _meta = {
        'case_insensitive_paths': False,
        'network': True,
        'read_only': True,
        'thread_safe': True,
        'virtual': True,
        'unicode_paths': False,
    }

    def __init__(self, base_url, cache_time=30):
        super(HTTPFS, self).__init__()
        self._base_url = base_url.rstrip('/')
        self._cache_time = cache_time
        self._info_cache = {}

    def _build_url(self, path):
        return '%s/%s' % (self._base_url, urlquote(path))

    def _getinfo(self, path):

        url = self._build_url(path)

        for attempt in (1, 2, 3):
            try:
                response = requests.head(url)
            except requests.RequestException as error:
                logging.warning('getinfo attempt %d: %s %s' % (attempt, url, error))
            else:
                break
        else:
            raise RemoteConnectionError('getinfo', path)

        if response.status_code == 200:
            if 'content-length' in response.headers:
                size_in_bytes = int(response.headers['content-length'])
                if 'last-modified' in response.headers:
                    last_modified = parse_http_date(response.headers['last-modified'])
                else:
                    last_modified = None
                file_time = last_modified or datetime.datetime.now()
                return {
                    'size': size_in_bytes,
                    'created_time': file_time,
                    'accessed_time': file_time,
                    'modified_time': file_time,
                }
            logging.warning('getinfo missing required headers: %s' % response.headers)
            raise RemoteConnectionError('getinfo', path)
        elif response.status_code in (301, 302, 403, 404):
            # Pretend that redirects and forbiddens are 404s.
            raise ResourceNotFoundError(path)
        else:
            logging.warning('getinfo status %d for %s assumed as connection error.' % (response.status_code, url))
            raise RemoteConnectionError('getinfo', path)

    def getinfo(self, path):

        cached = self._info_cache.get(path)
        if cached:
            expires, info, error = cached
            if expires > time.time():
                if error:
                    raise error
                else:
                    return info

        try:

            info = None
            error = None

            try:
                info = self._getinfo(path)
                return info
            except Exception as exception:
                error = exception
                raise

        finally:
            expires = time.time() + self._cache_time
            self._info_cache[path] = (expires, info, error)

    def isfile(self, path):
        try:
            self.getinfo(path)
        except ResourceNotFoundError:
            return False
        else:
            return True

    def isdir(self, path):
        return False

    def open(self, path, mode='r', **kwargs):

        if 'w' in mode or '+' in mode or 'a' in mode:
            logging.error('cannot use httpfs.open() in write mode: %s' % path)
            raise UnsupportedError('open', path=path)

        url = self._build_url(path)

        for attempt in (1, 2, 3):
            try:
                response = requests.get(url)
            except requests.RequestException as error:
                logging.warning('open attempt %d: %s %s' % (attempt, url, error))
            else:
                break
        else:
            raise RemoteConnectionError('getinfo', path)

        if response.status_code == 200:
            return StringIO(response.content)
        elif response.status_code == 404:
            raise ResourceNotFoundError(path)
        else:
            logging.warning('open status %d for %s assumed as connection error.' % (response.status_code, url))
            raise RemoteConnectionError('open', path)


def parse_http_date(date_string):
    """
    Converts a HTTP datetime string into a Python datatime object.
    Doesn't support every single format, but it's good enough.

    """
    try:
        return datetime.datetime(*parsedate(date_string)[:6])
    except Exception:
        return None

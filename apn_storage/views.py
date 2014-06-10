import datetime
import mimetypes
import os
import posixpath
import time
import urllib

from django.core.servers.basehttp import FileWrapper
from django.http import Http404, HttpResponse, HttpResponseRedirect, HttpResponseNotModified
from django.utils.http import http_date
from django.views.static import was_modified_since

from fs.errors import ResourceNotFoundError


def serve(request, path, document_root='', storage=None, stream=True):
    """
    This is a copy of django.views.static.serve except that it requires
    a ``storage`` param, and serves files from that storage object.

    It also attempts to open files rather than checking if they exist first.
    This allows custom filesystem wrappers to kick in.

    """

    if request.path.endswith('/'):
        raise Http404('Directory indexes are not allowed.')

    # Clean up given path to only allow serving files below document_root.
    path = posixpath.normpath(urllib.unquote(path))
    path = path.lstrip('/')
    newpath = ''
    for part in path.split('/'):
        if not part:
            # Strip empty path components.
            continue
        drive, part = os.path.splitdrive(part)
        head, part = os.path.split(part)
        if part in (os.curdir, os.pardir):
            # Strip '.' and '..' in path.
            continue
        newpath = os.path.join(newpath, part).replace('\\', '/')
    if newpath and path != newpath:
        return HttpResponseRedirect(newpath)

    fullpath = os.path.join(document_root, newpath)

    mimetype, encoding = mimetypes.guess_type(fullpath)
    mimetype = mimetype or 'application/octet-stream'

    # Respect the If-Modified-Since header.
    try:
        file_info = storage.fs.getinfo(fullpath)
    except (OSError, ResourceNotFoundError):
        file_info = None
    else:
        size = file_info['size']
        modified_time = file_info['modified_time']
        if isinstance(modified_time, datetime.datetime):
            modified_time = int(time.mktime(modified_time.timetuple()))
        if not was_modified_since(request.META.get('HTTP_IF_MODIFIED_SINCE'), modified_time, size):
            return HttpResponseNotModified(mimetype=mimetype)

    # Try to open the file without checking if it exists first. This allows
    # custom FS wrappers to activate for missing thumbnail images.
    try:
        open_file = storage.open(fullpath, 'rb')
    except (OSError, ResourceNotFoundError):
        raise Http404('"%s" does not exist' % fullpath)

    if file_info is None or not stream:
        # This is an automatically generated file. Read the
        # contents into memory here so the size can be determined.
        contents = open_file.read()
        open_file.close()
        size = len(contents)
        modified_time = int(time.time())
    else:
        # Stream the file contents without reading it into memory.
        contents = FileWrapper(open_file)

    response = HttpResponse(contents, mimetype=mimetype)
    response['Last-Modified'] = http_date(modified_time)
    response['Content-Length'] = size
    if encoding:
        response['Content-Encoding'] = encoding

    # Set a blank Etag to prevent the file contents from being read by
    # middleware just to generate it. This should be fine, because the
    # last-modified date is a good enough way to tell if a file has changed.
    response['ETag'] = ''

    return response

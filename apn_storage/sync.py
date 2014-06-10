import multiprocessing
import os
import logging
import Queue

from fs.errors import ParentDirectoryMissingError, ResourceNotFoundError

from apn_storage.utils import copy_file, walk_files


def sync_fs(source_fs, target_fs, delete_missing=False, processes=0, verbosity=0):
    """
    Synchronize two filesystems, so that target_fs will become like source_fs.

    Set delete_missing=True to remove files from the target_fs if not found
    on the source_fs.

    Set processes=<int> to enable multiprocessing worker processes. This can
    be useful for simultaneous uploads to S3, for example.

    """
    return sync_files(
        source_fs=source_fs,
        source_files=walk_files(source_fs),
        target_fs=target_fs,
        target_files=walk_files(target_fs),
        delete_missing=delete_missing,
        processes=processes,
        verbosity=verbosity,
    )


def sync_files(source_fs, source_files, target_fs, target_files, delete_missing=False, processes=0, verbosity=0):
    """
    Synchronize two filesystems, using iterables of WalkNode instances as
    the file lists. See sync_fs for more information.

    """

    actions = _get_sync_actions(
        source_files,
        target_files,
    )

    if not delete_missing:
        actions = (action for action in actions if action[0] is not delete_file)

    if processes:

        errored = multiprocessing.Event()
        finished = multiprocessing.Event()
        file_queue = multiprocessing.JoinableQueue(maxsize=1000)

        for x in range(processes):
            multiprocessing.Process(
                target=_process_files,
                kwargs={
                    'source_fs': source_fs,
                    'target_fs': target_fs,
                    'file_queue': file_queue,
                    'finished': finished,
                    'errored': errored,
                    'verbosity': verbosity,
                },
            ).start()

        for action in actions:
            file_queue.put(action)

        file_queue.join()
        finished.set()

        return not errored.is_set()

    else:

        for action, path in actions:

            if action is upload_file:
                upload_file(source_fs, target_fs, path, verbosity=verbosity)
            elif action is delete_file:
                delete_file(target_fs, path, verbosity=verbosity)
            elif action is skip_file:
                skip_file(path, verbosity=verbosity)
            else:
                raise ValueError(action)

        return True


def _get_sync_actions(source_files, target_files):
    """
    Iterates through the source_files and target_files, comparing the two,
    to decide what actions need to be taken in order to synchronize them.
    The arguments must be sequences of WalkNode instances.
    Returns tuples of (action_func, path)

    """

    sfile = next(source_files, None)
    tfile = next(target_files, None)

    while sfile or tfile:

        if sfile is None:

            # There are no more source files. All remaining target files are
            # files that don't exist on the source and can be deleted.
            yield (delete_file, tfile.path)
            tfile = next(target_files, None)

        elif tfile is None:

            # There are no more target files. All remaining source files are
            # files that don't exist on the target, and can be uploaded.
            yield (upload_file, sfile.path)
            sfile = next(source_files, None)

        elif sfile.path == tfile.path:

            # The same file exists in both places. If the filesizes are the
            # same, then assume the contents are too. Otherwise, replace it.
            if sfile.size == tfile.size:
                yield (skip_file, sfile.path)
            else:
                yield (upload_file, sfile.path)
            sfile = next(source_files, None)
            tfile = next(target_files, None)

        elif sfile.path > tfile.path:

            # The source path is further along than the target path.
            # This means that the target path doesn't exist on the source,
            # and it should be deleted.
            yield (delete_file, tfile.path)
            tfile = next(target_files, None)

        elif tfile.path > sfile.path:

            # The target path is further along than the source path.
            # This means that the source file doesn't exist on the target,
            # and it should be uploaded.
            yield (upload_file, sfile.path)
            sfile = next(source_files, None)

        else:
            raise ValueError('This should be logically impossible. %s vs %s' % (sfile, tfile))


def _process_files(source_fs, target_fs, file_queue, finished, errored, verbosity=0):
    """
    A function for multiprocessing worker processes to run.
    Reads actions from the file_queue and performs them.

    """
    while True:
        try:
            action, path = file_queue.get(timeout=1)
        except Queue.Empty:
            if finished.is_set():
                break
        else:
            try:
                if action is upload_file:
                    upload_file(source_fs, target_fs, path, verbosity=verbosity)
                elif action is delete_file:
                    delete_file(target_fs, path, verbosity=verbosity)
                elif action is skip_file:
                    skip_file(path, verbosity=verbosity)
                else:
                    raise ValueError(path)
            except Exception:
                errored.set()
                raise
            file_queue.task_done()


def upload_file(source_fs, target_fs, path, verbosity=0):
    """Copies a file from the source_fs to the target_fs."""
    logging.info('Uploading %r' % path, also_print=(verbosity >= 1))
    # There are some issues with copy_file when using multiprocessing and OSFS.
    # I'm not sure why it happens. For now, this just retries until it works.
    for x in xrange(100):
        try:
            copy_file(source_fs, path, target_fs, path, chunk_size=1024 * 1024)
        except ParentDirectoryMissingError:
            target_fs.makedir(os.path.dirname(path), recursive=True, allow_recreate=True)
        except ResourceNotFoundError:
            if source_fs.exists(path):
                target_fs.makedir(os.path.dirname(path), recursive=True, allow_recreate=True)
            elif source_fs.hassyspath(path):
                # Handle bad symlinks.
                syspath = source_fs.getsyspath(path)
                if os.path.islink(syspath) and not os.path.exists(syspath):
                    logging.warning('Bad symlink: %r' % syspath)
                    break
        else:
            break
    else:
        raise Exception('Error uploading %r' % path)


def delete_file(fs, path, verbosity=0):
    """Deletes a file from the fs."""
    logging.info('Deleting %r' % path, also_print=(verbosity >= 1))
    fs.remove(path)


def skip_file(path, verbosity=0):
    """Logs that the path is being skipped."""
    logging.info('OK: %r' % path, also_print=(verbosity >= 2))

from apn_storage import multifs
from apn_storage.utils import make_fs_from_string


def make_layered_fs(write_fs_string, *read_fs_strings):

    multifs_kwargs = []

    write_fs = make_fs_from_string(write_fs_string)
    multifs_kwargs.append({
        'name': 'layer1',
        'fs': write_fs,
        'write': True,
    })

    for layer_number, read_fs_string in enumerate(read_fs_strings, 2):
        read_fs = make_fs_from_string(read_fs_string)
        multifs_kwargs.append({
            'name': 'layer%d' % layer_number,
            'fs': read_fs,
            'write': False,
        })

    # Create the multi-layered filesystem object. Add the layers in
    # reverse order because that is how the multifs class requires it.
    # See http://pythonhosted.org/fs/multifs.html

    # Also, disable thread locking because we don't intend on updating
    # the child FS objects after this initalization step.

    multi_fs = multifs.MultiFS(thread_synchronize=False)
    for kwargs in reversed(multifs_kwargs):
        multi_fs.addfs(**kwargs)

    return multi_fs

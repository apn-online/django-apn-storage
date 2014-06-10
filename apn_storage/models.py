# TODO: consider how this might work in an app using this library.
# the storages won't exist in this library.
# so it might need some registration system.

# Also need the signals to work in apps without
# the before_test and after_test signals.

#all_storages = ()
#
#
#@receiver(signal=before_test)
#def enable_storage_test_mode(sender, **kwargs):
#    for storage in all_storages:
#        if hasattr(storage.fs, 'enable_test_mode'):
#            storage.fs.enable_test_mode()
#
#
#@receiver(signal=after_test)
#def disable_storage_test_mode(sender, **kwargs):
#    for storage in all_storages:
#        if hasattr(storage.fs, 'disable_test_mode'):
#            storage.fs.disable_test_mode()

"""
A tool for generating unique ID strings.

Uses a BSON Object ID for the underlying unique value generation.
See http://stackoverflow.com/a/5694803/2835599 for an explanation of "unique".

The class accepts an alphabet to use for the final string. This affects which
characters will be seen in the resulting UID strings, and also how long the
strings will be.

Some defaults (e.g. alphanumeric) have been provided so using the class
is not necessary.

Usage:

    from apn_storage import uid
    uid.alphanumeric()

Currently requires pymongo and NOT bson, because pymongo
includes a different version of the bson module. I think.

"""

import bson
import string


class UIDGenerator(object):

    def __init__(self, alphabet):
        """Create a new UID generator using the provided alphabet."""

        self.alphabet = str(alphabet)

        self._alphabet_length = len(alphabet)
        if self._alphabet_length < 2:
            raise ValueError('The alphabet must container 2 or more characters.')

        if len(set(self.alphabet)) != self._alphabet_length:
            raise ValueError('The alphabet contained duplicate characters.')

    def __call__(self):
        """Generate a new unique string."""
        unique_hex = str(bson.ObjectId())
        unique_int = int(unique_hex, 16)
        output = ''
        while unique_int:
            unique_int, index = divmod(unique_int, self._alphabet_length)
            output += self.alphabet[index]
        return output

    @property
    def length(self):
        """The length of generated strings."""
        if not hasattr(self, '_length'):
            self._length = len(self())
        return self._length


alphanumeric = UIDGenerator(string.digits + string.ascii_letters)
alphanumeric_lowercase = UIDGenerator(string.digits + string.ascii_lowercase)

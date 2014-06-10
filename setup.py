#!/usr/bin/env python

from setuptools import setup

setup(
    name='django-apn-storage',
    version='0.0.1',
    description='An extension of pyfilesystem that provides extra features in Django.',
    author='Raymond Butcher',
    author_email='randomy@gmail.com',
    url='https://github.com/apn-online/django-apn-storage',
    license='MIT',
    packages=(
        'apn_storage',
    ),
    install_requires=(
        'django',
        'fs == 0.5',
        'lazyobject',
    ),
)

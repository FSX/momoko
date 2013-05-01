#!/usr/bin/env python

import os

# The multiprocessingimport is to prevent the following error
# after all the tests have been executed.
# Error in atexit._run_exitfuncs:
# TypeError: 'NoneType' object is not callable

# From: http://article.gmane.org/gmane.comp.python.peak/2509
# Work around setuptools bug
# http://article.gmane.org/gmane.comp.python.peak/2509
import multiprocessing

try:
    from setuptools import setup, Extension, Command
except ImportError:
    from distutils.core import setup, Extension, Command


dependencies = ['tornado']
psycopg2_impl = os.environ.get('MOMOKO_PSYCOPG2_IMPL', 'psycopg2')

if psycopg2_impl == 'psycopg2cffi':
    print('Using psycopg2cffi')
    dependencies.append('psycopg2cffi')
elif psycopg2_impl == 'psycopg2ct':
    print('Using psycopg2ct')
    dependencies.append('psycopg2ct')
else:
    print('Using psycopg2')
    dependencies.append('psycopg2')


setup(
    name='Momoko',
    version='1.0.0',
    description="Momoko wraps Psycopg2's functionality for use in Tornado.",
    long_description=open('README.rst').read(),
    author='Frank Smit',
    author_email='frank@61924.nl',
    url='http://momoko.61924.nl/',
    packages=['momoko'],
    license='MIT',
    test_suite='tests',
    install_requires=dependencies,
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Topic :: Database',
        'Topic :: Database :: Front-Ends'
    ]
)

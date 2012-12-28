#!/usr/bin/env python

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


setup(
    name='Momoko',
    version='1.0.0b2',
    description='Wraps (asynchronous) Psycopg2 for Tornado.',
    long_description=open('README.rst').read(),
    author='Frank Smit',
    author_email='frank@61924.nl',
    url='http://momoko.61924.nl/',
    packages=['momoko'],
    license='MIT',
    test_suite = 'momoko.tests',
    install_requires=[
        'tornado',
        'psycopg2'
    ],
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Topic :: Database',
        'Topic :: Database :: Front-Ends'
    ]
)

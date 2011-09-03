#!/usr/bin/env python

from distutils.core import setup


setup(
    name='Momoko',
    version='0.4.0',
    description='An asynchronous Psycopg2 wrapper for Tornado.',
    long_description=open('README.txt').read(),
    author='Frank Smit',
    author_email='frank@61924.nl',
    url='http://momoko.61924.nl/',
    packages=['momoko'],
    license='MIT',
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

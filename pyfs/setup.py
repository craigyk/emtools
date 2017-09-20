#!/usr/bin/env python

from setuptools import setup

setup( name='pyfs',
       version='1.0.0',
       packages=['pyfs'],
       install_requires = [
            'git+https://bitbucket.org/craigyk/oneup.git',
       ]
)

#!/usr/bin/env python
from distutils.core import setup

setup(
    name='Monglue',
    version='0.0a',
    description='Minimal MongoDB document manager and toolkit',
    url='http://github.com/thedaniel/monglue',
    requires=[
        'pymongo>=1.9',
        ],
    extras_require=dict(test=['nose','fudge',]),
    )

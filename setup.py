# -*- coding: utf-8 -*-
from setuptools import setup


TOP_LEVEL = 'lamport_mutex'
VERSION_PATH = '{}/version.py'.format(TOP_LEVEL)


VERSION = '0.0.0.develop'


if __name__ == '__main__':
    with open(VERSION_PATH, 'w') as output:
        output.write('__version__ = {!r}\n'.format(VERSION))

    setup(
        name='lamport-mutex',
        version=VERSION,
        author='Vladimir Goncharov',
        author_email='dev.zelta@gmail.com',
        entry_points=dict(
            console_scripts=[
                'lamport-mutex=lamport_mutex.cli:main',
                'lm=lamport_mutex.cli:main',
            ]
        )
    )

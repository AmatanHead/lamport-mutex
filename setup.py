# -*- coding: utf-8 -*-
from setuptools import setup, find_packages


TOP_LEVEL = 'lamport_mutex'
VERSION_PATH = '{}/version.py'.format(TOP_LEVEL)


VERSION = '0.0.0.develop'


if __name__ == '__main__':
    with open(VERSION_PATH, 'w') as output:
        output.write('__version__ = {!r}\n'.format(VERSION))

    setup(
        name='lamport-mutex',
        version=VERSION,
        packages=find_packages(),
        author='Vladimir Goncharov',
        author_email='dev.zelta@gmail.com',
        extras_require={
            'dev': [
                'pytest==3.0.5',
                'pytest-asyncio==0.5.0',
            ]
        },
        entry_points=dict(
            console_scripts=[
                'lamport-mutex=lamport_mutex.cli:main',
                'lm=lamport_mutex.cli:main',
            ]
        )
    )

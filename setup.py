#!/usr/bin/env python3

from setuptools import setup

setup(
    name='reclada.coordinator',
    description='Dict Extractor step for Reclada Parser',
    version='0.1',
    packages=['reclada.coordinator'],
    install_requires=[
        'reclada.connector',
        'luigi',
        'requests',
        'boto3',
    ],
    entry_points={
        'console_scripts': ['reclada-coordinator=reclada.coordinator.main:run'],
    },
    python_requires='>=3.6',
)

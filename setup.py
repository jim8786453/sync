#!/usr/bin/env python
import os

from setuptools import setup
from pip.req import parse_requirements

install_reqs = parse_requirements('requirements.txt', session=False)
reqs = [str(ir.req) for ir in install_reqs]
del os.link

setup(
    author='Jim Kennedy',
    author_email='jim8786453@gmail.com',
    description='Synchronise data',
    install_requires=reqs,
    name='sync',
    packages=['sync'],
    version='0.0.1',
)

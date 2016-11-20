#!/usr/bin/env python
import os
import platform

from setuptools import setup
from pip.req import parse_requirements


req_file = 'requirements.txt'
if platform.python_implementation() != 'CPython':
    req_file = 'requirementsPyPy.txt'
install_reqs = parse_requirements(req_file, session=False)
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

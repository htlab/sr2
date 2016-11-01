# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name='sr2',
    version='0.0.1',
    description='sox recorder 2',
    author='Hide. Tokuda Lab.',
    author_email='contact@ht.sfc.keio.ac.jp',
    url='https://github.com/htlab/sr2',
    packages=find_packages(),
    license='closed',
    include_package_data=True,
    install_requires=[
    ],
    tests_require=['nose', 'WebTest'],
    test_suite='nose.collector'
)

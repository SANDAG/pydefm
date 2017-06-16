"""A setuptools based setup module.
See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/sandag/pydefm
"""

from setuptools import setup, find_packages
from codecs import open
from os import path
import pydefm


setup(
    name='pydefm',
    version='0.1',
    description='San Diego Demographic and Economic Model',
    long_description = long_description,
    author='San Diego Association of Governments (SANDAG)',
    license='BSD',
    classifiers= [
        'Development Status :: 5 - Production / Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Utilities',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7'
    ],
    packages=find_packages(),
    install_requires = [],
    test_suite='test'
)







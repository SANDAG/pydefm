from setuptools import setup, find_packages

with open('README.md') as file:
    long_description= file.read()

setup(
    name='pydefm',
    version='0.1',
    description='San Diego Demographic and Economic Model',
    author='San Diego Association of Governments',
    license='BSD-3',
    url='',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 2.7',
        'License :: OSI Approved :: BSD License'
    ],
    long_description=long_description,
    packages=find_packages(exclude=['*.tests'])
)

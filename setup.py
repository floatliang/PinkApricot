# -*- coding: utf-8 -*-
# @Time    : 2021/06/21 18:00
# @Author  : floatsliang@gmail.com
# @File    : setup.py

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()
setup(
    name='PinkApricot',
    version="0.0.4",
    description='PinkApricot is configurable sql exporter not only export metrics but also love :)',
    packages=find_packages(exclude=['test']),
    url='https://github.com/floatliang/PinkApricot',
    author='floatsliang@gmail.com',
    author_email='floatsliang@gmail.com',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    keywords='prometheus configurable sql exporter',
    install_requires=[
        'Flask >= 2.0.1',
        'PyMySQL >= 1.0.1',
        'dsnparse >= 0.1.15',
        "prometheus_client >= 0.11.0"
    ],
    zip_safe=False,
    platforms=['any'],
)
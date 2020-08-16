#!/usr/bin/env python
# coding: utf-8
from setuptools import setup, find_packages


setup(
    name="alexflow",
    version="1.0.0",
    description="ALEXFlow is a python workflow library built for reproducible complex workflow",
    license="MIT",
    author="Sho Yoshida",
    author_email="yoshiso@alpaca.ai",
    url="https://github.com/AlpacaDB/alexflow.git",
    keywords="",
    packages=find_packages(exclude=('tests',)),
    python_requires='>=3.7',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    install_requires=[
        "pandas",
        "numpy",
        "dataclass-serializer>=1.3.1",
        "multiprocess",
        "joblib",
        "cached-property",
    ],
)
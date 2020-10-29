#!/usr/bin/env python
# coding: utf-8
from setuptools import setup, find_packages

# read the contents of your README file
from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


setup(
    name="alexflow",
    version="1.1.4",
    description="ALEXFlow is a python workflow library built for reproducible complex workflow",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="Sho Yoshida",
    author_email="yoshiso@alpaca.ai",
    url="https://github.com/AlpacaDB/alexflow.git",
    keywords="",
    packages=find_packages(exclude=("tests",)),
    python_requires=">=3.7",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
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

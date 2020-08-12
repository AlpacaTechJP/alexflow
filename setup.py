#!/usr/bin/env python
# coding: utf-8
from setuptools import setup, find_packages


setup(
    name="alexflow",
    version="1.0.0",
    description="Workflow library built for compositional tasks",
    license="MIT",
    author="Sho Yoshida",
    author_email="yoshiso@alpaca.ai",
    url="https://github.com/AlpacaDB/alexflow.git",
    keywords="",
    packages=find_packages(include=("alexflow",)),
    install_requires=[
        "pandas",
        "numpy",
        "dataclass-serializer>=1.3.1",
        "multiprocess",
        "joblib",
        "cached-property",
    ],
)
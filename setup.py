#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="tap-shipstation",
    version="0.1.0",
    description="Singer.io tap for extracting data from the ShipStation API",
    author="Josh Temple",
    url="https://github.com/fixdauto/tap-shipstation",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    packages=find_packages(),
    install_requires=[
        "singer-python>=5.2.0",
        "requests>=2.20.0",
        "pendulum>=2.0.5",
        "jsonref>=0.2"
    ],
    entry_points="""
    [console_scripts]
    tap-shipstation=tap_shipstation:main
    """,
    include_package_data=True,
)
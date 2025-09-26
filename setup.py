#!/usr/bin/env python
from setuptools import setup, find_packages

# This setup.py is based on the original tap-shipstation structure
# but updated for the v2 tap's package name and dependencies.

setup(
    # Match the original PR's name if this is an update,
    # or use the new name if it's a separate plugin.
    # Using the new name for clarity.
    name="tap-shipstation-fixed-v2",
    version="0.1.0",
    description="Singer.io tap for extracting data from the ShipStation API (v2, hardened)",
    # It's good practice to update the author if you are the primary contributor to the new version.
    author="Josh Temple & Nick Chang",
    url="https://github.com/fixdauto/tap-shipstation", # Pointing to the main repo PR
    classifiers=["Programming Language :: Python :: 3 :: Only"],

    # find_packages() is a modern way to automatically find your code package.
    # It will find the 'tap_shipstation_fixed_v2' directory.
    packages=find_packages(),

    # Using the updated and more specific dependency versions from your new setup.py
    # is better for ensuring stability.
    install_requires=[
        "singer-python>=5.2.0",
        "requests>=2.20.0",
        "pendulum>=2.0.5",
        "jsonref>=0.2"
    ],


    entry_points="""
    [console_scripts]
    tap-shipstation-fixed-v2=tap_shipstation_fixed_v2:main
    """,
    include_package_data=True,
)
#!/usr/bin/env python
import os
from distutils.core import setup

from setuptools import find_packages

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()


package_name = "dbt-athena"
dbt_minimal_version = "0.16.1"
version = "0.0.4"
description = """The athena adpter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Fishtown Analytics",
    author_email="info@fishtownanalytics.com",
    url="https://github.com/fishtown-analytics/dbt",
    packages=find_packages(),
    package_data={
        "dbt": [
            "include/athena/dbt_project.yml",
            "include/athena/macros/*.sql",
            "include/athena/macros/*/*.sql",
        ]
    },
    install_requires=["dbt-core>={}".format(dbt_minimal_version), "PyAthena"],
)

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# flake8: noqa
"""The setup script."""
from setuptools import find_packages
from setuptools import setup

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

# here we have listed all dependencies w/o explicit pins to enable flexibility in client installs.
# we use `requirements.txt` in this directory when testing to ensure a stable test in CI.
requirements = [
    "attrs",  # features we use are not regularly changing
    "botocore",  # features we use are not regularly changing
    "Click<9.0.0,>6.0.0",  # pinning to 7.x or 8.x as we have used w/ both
    "confuse==1.5.0",  # important for config so don't change w/o testing
    "desert",  # features we use are not regularly changing
    "marshmallow",  # features we use are not regularly changing
    "marshmallow_oneofschema",  # features we use are not regularly changing
    "python-dateutil",  # stable
    "requests",  # stable
    "requests-aws4auth",  # stable
    "simplejson",  # stable
]

setup_requirements = ["pytest-runner", "pip"]

setup(
    author="Ryan Murray",
    author_email="nessie-release-builder@dremio.com",
    python_requires=">=3.5",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    description="Project Nessie: A Git-like Experience for your Data Lake",
    entry_points={
        "console_scripts": [
            "nessie=pynessie.cli:cli",
        ],
    },
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + "\n" + history,
    include_package_data=True,
    keywords="pynessie",
    name="pynessie",
    packages=find_packages(include=["pynessie", "pynessie.*"]),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=[],
    url="https://github.com/projectnessie/nessie",
    version="0.7.1",
    zip_safe=False,
)

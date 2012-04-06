from setuptools import setup, find_packages
import sys, os

version = "0.2"

setup(
    name="saimmutable",
    version=version,
    description="SA instrumentation machinery for immutable data models",
    author="Andrey Popp",
    author_email="8mayday@gmail.com",
    license="BSD",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    include_package_data=True,
    zip_safe=False,
    test_suite="tests",
    install_requires=["SQLAlchemy >= 0.7.5"])

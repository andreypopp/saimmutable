from setuptools import setup, find_packages
import sys, os

version = "0.1"

setup(
    name="sqlalchemyro",
    version=version,
    description="Alternative instrumentation machinery for read-only models",
    author="Andrey Popp",
    author_email="8mayday@gmail.com",
    license="BSD",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    include_package_data=True,
    zip_safe=False,
    test_suite="tests",
    install_requires=[
        "SQLAlchemy >= 0.7.5",
    ],
    entry_points="""
    # -*- Entry points: -*-
    """)

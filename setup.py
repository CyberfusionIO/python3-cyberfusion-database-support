"""A setuptools based setup module."""

from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="python3-cyberfusion-database-support",
    version="2.5.4.8",
    description="Library for MariaDB and PostgreSQL.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Cyberfusion",
    author_email="support@cyberfusion.io",
    url="https://github.com/CyberfusionIO/python3-cyberfusion-database-support",
    platforms=["linux"],
    packages=[
        "cyberfusion.DatabaseSupport",
        "cyberfusion.DatabaseSupport.exceptions",
    ],
    package_dir={"": "src"},
    data_files=[],
    install_requires=[
        "cached_property==1.5.2",
        "psycopg2==2.9.5",
        "PyMySQL==1.0.2",
        "SQLAlchemy==1.4.46",
        "SQLAlchemy-Utils==0.38.2",
        "python3-cyberfusion-common~=2.10",
    ],
)

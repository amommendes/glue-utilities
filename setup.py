import os
from setuptools import setup, find_packages

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="glue_migrator",
    version="0.0.1",
    author="Amom Mendes",
    author_email="amommendes@gmail.com",
    description="Migrates Glue databases to external Metastore",
    license="APACHE2.0",
    keywords="glue metastore",
    packages=find_packages(exclude=['tests']),
    long_description=read("README.md"),
)

import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "glue_migrator",
    version = "0.0.1",
    author = "Amom Mendes",
    author_email = "amommendes@gmail.com",
    description ="Migrates Glue databases to external Metastore",
    license = "APACHE",
    keywords = "glue metastore",
    packages=['glue_migrator'],
    long_description=read('README')
)
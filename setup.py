# -*- coding: utf-8 -*-

import re

from Cython.Build import cythonize
from setuptools import Extension

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def find_version():
    for line in open("asynctnt/__init__.py"):
        if line.startswith("__version__"):
            return re.match(r"""__version__\s*=\s*(['"])([^'"]+)\1""", line).group(2)

setup(
    name="asynctnt",
    packages=["asynctnt"],
    # ext_modules=cythonize([Extension("asynctnt.protocol", ["asynctnt/protocol.pyx"])]),
    version=find_version(),
    author="igorcoding",
    author_email="igorcoding@gmail.com",
    classifiers=[
        "Programming Language :: Python :: 3.5",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database :: Front-Ends"
    ],
    install_requires=[
        "tarantool>=0.5.1",
    ],
    description="Tarantool connection driver for work with asyncio",
    # long_description=open("README.rst").read()
)

# -*- coding: utf-8 -*-
import os
import re
import unittest

from setuptools import Extension

from setuptools.command import build_ext as _build_ext

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

description = "A fast Tarantool Database connector for Python/asyncio."
try:
    import pypandoc
    long_description = pypandoc.convert_file('README.md', 'rst')
except (IOError, ImportError):
    long_description = description


def find_version():
    for line in open("asynctnt/__init__.py"):
        if line.startswith("__version__"):
            return re.match(
                r"""__version__\s*=\s*(['"])([^'"]+)\1""", line).group(2)


class build_ext(_build_ext.build_ext):
    user_options = _build_ext.build_ext.user_options + [
        ('cython-always', None,
            'run cythonize() even if .c files are present'),
        ('cython-annotate', None,
            'Produce a colorized HTML version of the Cython source.'),
        ('cython-directives=', None,
            'Cythion compiler directives'),
    ]

    def initialize_options(self):
        super(build_ext, self).initialize_options()
        self.cython_always = False
        self.cython_annotate = None
        self.cython_directives = None

    def finalize_options(self):
        need_cythonize = self.cython_always
        cfiles = {}

        for extension in self.distribution.ext_modules:
            for i, sfile in enumerate(extension.sources):
                if sfile.endswith('.pyx'):
                    prefix, ext = os.path.splitext(sfile)
                    cfile = prefix + '.c'

                    if os.path.exists(cfile) and not self.cython_always:
                        extension.sources[i] = cfile
                    else:
                        if os.path.exists(cfile):
                            cfiles[cfile] = os.path.getmtime(cfile)
                        else:
                            cfiles[cfile] = 0
                        need_cythonize = True

        if need_cythonize:
            try:
                import Cython
            except ImportError:
                raise RuntimeError(
                    'please install Cython to compile asynctnt from source')

            import pkg_resources
            cython_dep = pkg_resources.Requirement.parse('0.28.4')
            if Cython.__version__ not in cython_dep:
                raise RuntimeError(
                    'asynctnt requires Cython version 0.28.4')

            from Cython.Build import cythonize

            directives = {}
            if self.cython_directives:
                for directive in self.cython_directives.split(','):
                    k, _, v = directive.partition('=')
                    if v.lower() == 'false':
                        v = False
                    if v.lower() == 'true':
                        v = True

                    directives[k] = v

            self.distribution.ext_modules[:] = cythonize(
                self.distribution.ext_modules,
                compiler_directives=directives,
                annotate=self.cython_annotate)

        super(build_ext, self).finalize_options()

setup(
    name="asynctnt",
    packages=["asynctnt"],
    include_package_data=True,
    cmdclass={'build_ext': build_ext},
    ext_modules=[
        Extension("asynctnt.iproto.protocol",
                  sources=[
                      "asynctnt/iproto/protocol.pyx",
                      "third_party/msgpuck/msgpuck.c",
                      "third_party/msgpuck/hints.c",
                  ],
                  include_dirs=[
                      '-Ithird_party'
                  ])
    ],
    version=find_version(),
    author="igorcoding",
    author_email="igorcoding@gmail.com",
    url='https://github.com/igorcoding/asynctnt',
    license='Apache Software License',
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database :: Front-Ends"
    ],
    install_requires=[
        'PyYAML>=3.13'
    ],
    description=description,
    long_description=long_description,
    test_suite='run_tests.discover_tests'
)

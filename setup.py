# -*- coding: utf-8 -*-
import os
import re

from setuptools import Extension

from setuptools.command import build_ext as _build_ext

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

description = "A fast Tarantool Database connector for Python/asyncio."
with open('README.md') as f:
    long_description = f.read()


def find_version():
    for line in open("asynctnt/__init__.py"):
        if line.startswith("__version__"):
            return re.match(
                r"""__version__\s*=\s*(['"])([^'"]+)\1""", line).group(2)


CYTHON_VERSION = '0.29.21'


class build_ext(_build_ext.build_ext):
    user_options = _build_ext.build_ext.user_options + [
        ('cython-always', None,
            'run cythonize() even if .c files are present'),
        ('cython-annotate', None,
            'Produce a colorized HTML version of the Cython source.'),
        ('cython-directives=', None,
            'Cython compiler directives'),
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
            cython_dep = pkg_resources.Requirement.parse(CYTHON_VERSION)
            if Cython.__version__ not in cython_dep:
                raise RuntimeError(
                    'asynctnt requires Cython version {}'.format(
                        CYTHON_VERSION))

            from Cython.Build import cythonize

            directives = {
                'language_level': '3'
            }
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
                      "asynctnt/iproto/tupleobj/tupleobj.c"
                  ],
                  include_dirs=[
                      'third_party',
                      'asynctnt/iproto',
                  ])
    ],
    version=find_version(),
    author="igorcoding",
    author_email="igorcoding@gmail.com",
    url='https://github.com/igorcoding/asynctnt',
    license='Apache Software License',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        'Programming Language :: Python :: Implementation :: CPython',
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database :: Front-Ends"
    ],
    install_requires=[
        "PyYAML >= 5.0"
    ],
    setup_requires=[
        "Cython=={}".format(CYTHON_VERSION)
    ],
    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',
    test_suite='run_tests.discover_tests'
)

# -*- coding: utf-8 -*-
import os
import re

import setuptools
from setuptools.command import build_ext as setuptools_build_ext


def find_version():
    for line in open("asynctnt/__init__.py"):
        if line.startswith("__version__"):
            return re.match(r"""__version__\s*=\s*(['"])([^'"]+)\1""", line).group(2)


CYTHON_VERSION = "3.0.7"


class build_ext(setuptools_build_ext.build_ext):
    user_options = setuptools_build_ext.build_ext.user_options + [
        ("cython-always", None, "run cythonize() even if .c files are present"),
        (
            "cython-annotate",
            None,
            "Produce a colorized HTML version of the Cython source.",
        ),
        ("cython-directives=", None, "Cython compiler directives"),
    ]

    def initialize_options(self):
        if getattr(self, "_initialized", False):
            return

        super(build_ext, self).initialize_options()

        if os.environ.get("ASYNCTNT_DEBUG"):
            self.cython_always = True
            self.cython_annotate = True
            self.cython_directives = {
                "linetrace": True,
            }
            self.define = "CYTHON_TRACE,CYTHON_TRACE_NOGIL"
            self.debug = True
            self.gdb_debug = True
        else:
            self.cython_always = False
            self.cython_annotate = None
            self.cython_directives = None
            self.gdb_debug = False

    def finalize_options(self):
        if getattr(self, "_initialized", False):
            return

        need_cythonize = self.cython_always

        if not need_cythonize:
            for extension in self.distribution.ext_modules:
                for i, sfile in enumerate(extension.sources):
                    if sfile.endswith(".pyx"):
                        prefix, ext = os.path.splitext(sfile)
                        cfile = prefix + ".c"

                        if os.path.exists(cfile) and not self.cython_always:
                            extension.sources[i] = cfile
                        else:
                            need_cythonize = True

        if need_cythonize:
            self.cythonize()

        super(build_ext, self).finalize_options()

    def cythonize(self):
        try:
            import Cython
        except ImportError as e:
            raise RuntimeError(
                "please install Cython to compile asynctnt from source"
            ) from e

        if Cython.__version__ != CYTHON_VERSION:
            raise RuntimeError(
                "asynctnt requires Cython version {}, got {}".format(
                    CYTHON_VERSION, Cython.__version__
                )
            )

        from Cython.Build import cythonize

        directives = {"language_level": "3"}
        if self.cython_directives:
            if isinstance(self.cython_directives, str):
                for directive in self.cython_directives.split(","):
                    k, _, v = directive.partition("=")
                    if v.lower() == "false":
                        v = False
                    if v.lower() == "true":
                        v = True

                    directives[k] = v
            elif isinstance(self.cython_directives, dict):
                directives.update(self.cython_directives)

        self.distribution.ext_modules[:] = cythonize(
            self.distribution.ext_modules,
            compiler_directives=directives,
            annotate=self.cython_annotate,
            gdb_debug=self.gdb_debug,
        )


setuptools.setup(
    version=find_version(),
    cmdclass={"build_ext": build_ext},
    ext_modules=[
        setuptools.extension.Extension(
            "asynctnt.iproto.protocol",
            sources=[
                "asynctnt/iproto/protocol.pyx",
                "third_party/msgpuck/msgpuck.c",
                "third_party/msgpuck/hints.c",
                "asynctnt/iproto/tupleobj/tupleobj.c",
            ],
            include_dirs=[
                "third_party",
                "asynctnt/iproto",
            ],
        )
    ],
)

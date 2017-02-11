.PHONY: build debug test coverage clean annotate all


PYTHON ?= python


all: build


clean:
	rm -rf asynctnt/*.c asynctnt/*.so asynctnt/*.html
	rm -rf asynctnt/iproto/*.c asynctnt/iproto/*.so asynctnt/iproto/*.html
	rm -rf build *.egg-info
	find . -name '__pycache__' | xargs rm -rf


annotate:
	cython -a asynctnt/iproto/protocol.pyx


style:
	pep8 asynctnt
	flake8 --config=.flake8.cython


build:
	$(PYTHON) setup.py build_ext --inplace --cython-always


debug: clean
	$(PYTHON) setup.py build_ext --inplace --debug \
		--cython-always \
		--cython-annotate \
		--cython-directives="linetrace=True" \
		--define CYTHON_TRACE,CYTHON_TRACE_NOGIL


test:
	PYTHONASYNCIODEBUG=1 $(PYTHON) -m unittest discover -s tests
	$(PYTHON) -m unittest discover -s tests
	#USE_UVLOOP=1 $(PYTHON) -m unittest discover -s tests


coverage: debug
	pip install -e .
	coverage run run_tests.py
	./scripts/run_until_success.sh coverage report -m
	./scripts/run_until_success.sh coverage html


sdist: clean build test
	$(PYTHON) setup.py sdist


release: clean build test
	$(PYTHON) setup.py sdist upload

.PHONY: build debug test coverage clean annotate all


PYTHON ?= python


all: build


clean:
	rm -rf asynctnt/*.c asynctnt/*.so asynctnt/*.html
	rm -rf asynctnt/iproto/*.c asynctnt/iproto/*.so asynctnt/iproto/*.html
	rm -rf build *.egg-info
	find . -name '__pycache__' | xargs rm -rf
	rm -rf htmlcov


build:
	$(PYTHON) setup.py build_ext --inplace --cython-always


debug: clean
	$(PYTHON) setup.py build_ext --inplace --debug \
		--cython-always \
		--cython-annotate \
		--cython-directives="linetrace=True" \
		--define CYTHON_TRACE,CYTHON_TRACE_NOGIL


annotate:
	cython -a asynctnt/iproto/protocol.pyx


test:
	PYTHONASYNCIODEBUG=1 $(PYTHON) -m unittest discover -v -s tests
	$(PYTHON) -m unittest discover -v -s tests
	USE_UVLOOP=1 $(PYTHON) -m unittest discover -v -s tests


quicktest:
	$(PYTHON) -m unittest discover -v -s tests


test_16:
	TARANTOOL_DOCKER_VERSION=1.6 $(PYTHON) -m unittest discover -s tests


test_17:
	TARANTOOL_DOCKER_VERSION=1.7 $(PYTHON) -m unittest discover -s tests


coverage:
	# pip install -e .
	coverage run run_tests.py
	./scripts/run_until_success.sh coverage report -m
	./scripts/run_until_success.sh coverage html


style:
	flake8 --config=.flake8


sdist: clean build test
	$(PYTHON) setup.py sdist


release: clean build test
	$(PYTHON) setup.py sdist upload


docs: build
	$(MAKE) -C docs html

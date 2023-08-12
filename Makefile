.PHONY: clean build local debug annotate dist docs style mypy ruff style-check lint test quicktest coverage

PYTHON?=python

all: local

clean:
	pip uninstall -y asynctnt
	rm -rf asynctnt/*.c asynctnt/*.h asynctnt/*.cpp
	rm -rf asynctnt/*.so asynctnt/*.html
	rm -rf asynctnt/iproto/*.c asynctnt/iproto/*.h
	rm -rf asynctnt/iproto/*.so asynctnt/iproto/*.html asynctnt/iproto/requests/*.html
	rm -rf build *.egg-info .eggs dist
	find . -name '__pycache__' | xargs rm -rf
	rm -rf htmlcov
	rm -rf __tnt*
	rm -rf tests/__tnt*


build:
	$(PYTHON) -m pip install -e '.[test,docs]'

local:
	$(PYTHON) -m pip install -e .


debug: clean
	ASYNCTNT_DEBUG=1 $(PYTHON) -m pip install -e '.[test]'


annotate:
	cython -3 -a asynctnt/iproto/protocol.pyx

dist:
	$(PYTHON) -m build .

docs: build
	$(MAKE) -C docs html

style:
	$(PYTHON) -m black .
	$(PYTHON) -m isort .

mypy:
	$(PYTHON) -m mypy --enable-error-code ignore-without-code .

ruff:
	$(PYTHON) -m ruff .

style-check:
	$(PYTHON) -m black --check --diff .
	$(PYTHON) -m isort --check --diff .

lint: style-check ruff

test: lint
	PYTHONASYNCIODEBUG=1 $(PYTHON) -m pytest
	$(PYTHON) -m pytest
	USE_UVLOOP=1 $(PYTHON) -m pytest

quicktest:
	$(PYTHON) -m pytest

coverage:
	$(PYTHON) -m pytest --cov
	./scripts/run_until_success.sh $(PYTHON) -m coverage report -m
	./scripts/run_until_success.sh $(PYTHON) -m coverage html

.PHONY: clean build local debug annotate dist docs style mypy ruff style-check lint test quicktest coverage

all: local

build:
	uv pip install -e '.[test,docs]'

local:
	uv pip install -e .

debug: clean
	ASYNCTNT_DEBUG=1 uv pip install -e '.[test]'

annotate:
	cython -3 -a asynctnt/iproto/protocol.pyx

lint: style-check ruff

style:
	uv run --active ruff format .
	uv run --active ruff check --select I,F401 --fix .

style-check:
	uv run --active ruff format --check .

ruff:
	uv run --active ruff check .

mypy:
	uv run --active mypy --enable-error-code ignore-without-code .

test:
	PYTHONASYNCIODEBUG=1 uv run --active pytest
	uv run --active pytest
	USE_UVLOOP=1 uv run --active pytest

quicktest:
	uv run --active pytest

coverage:
	uv run --active pytest --cov
	./scripts/run_until_success.sh uv run --active coverage report -m
	./scripts/run_until_success.sh uv run --active coverage html

dist:
	uv pip install build
	uv run --active python -m build .

docs: build
	$(MAKE) -C docs html

clean:
	uv pip uninstall asynctnt
	rm -rf asynctnt/*.c asynctnt/*.h asynctnt/*.cpp
	rm -rf asynctnt/*.so asynctnt/*.html
	rm -rf asynctnt/iproto/*.c asynctnt/iproto/*.h
	rm -rf asynctnt/iproto/*.so asynctnt/iproto/*.html asynctnt/iproto/requests/*.html
	rm -rf build *.egg-info .eggs dist
	find . -name '__pycache__' | xargs rm -rf
	rm -rf htmlcov
	rm -rf __tnt*
	rm -rf tests/__tnt*

.PHONY: build debug test quicktest clean all


PYTHON ?= python


all: build


clean:
	rm -rf asynctnt/*.c asynctnt/*.so asynctnt/*.html
	rm -rf asynctnt/ciproto/*.c asynctnt/ciproto/*.so asynctnt/ciproto/*.html
	rm -rf build *.egg-info
	find . -name '__pycache__' | xargs rm -rf


build:
	$(PYTHON) setup.py build_ext --inplace --cython-always


debug:
	$(PYTHON) setup.py build_ext --inplace --debug \
		--cython-always \
		--cython-annotate \
		--cython-directives="linetrace=True" \
		--define CYTHON_TRACE,CYTHON_TRACE_NOGIL


test:
	PYTHONASYNCIODEBUG=1 $(PYTHON) -m unittest discover -s tests
	$(PYTHON) -m unittest discover -s tests
	USE_UVLOOP=1 $(PYTHON) -m unittest discover -s tests


quicktest:
	$(PYTHON) -m unittest discover -s tests


sdist: clean build test
	$(PYTHON) setup.py sdist


release: clean build test
	$(PYTHON) setup.py sdist upload

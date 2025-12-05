.PHONY: test build clean install dev

test:
	pytest -v --cov=object_storage --cov-report=term-missing

build:
	python -m build

clean:
	rm -rf build dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete

install:
	pip install -e .

dev:
	pip install -e ".[dev]"

all: dev test build

.PHONY: help install dev-install test lint format clean build upload

help:
	@echo "Available commands:"
	@echo "  install      Install the package"
	@echo "  dev-install  Install the package with development dependencies"
	@echo "  test         Run tests"
	@echo "  lint         Run linters (flake8, mypy)"
	@echo "  format       Format code (black, isort)"
	@echo "  clean        Clean build artifacts"
	@echo "  build        Build distribution packages"
	@echo "  upload       Upload to PyPI"

install:
	pip install -e .

dev-install:
	pip install -e ".[dev]"

test:
	pytest

lint:
	flake8 feishu_bitable_db tests
	mypy feishu_bitable_db

format:
	black feishu_bitable_db tests
	isort feishu_bitable_db tests

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	python -m build

upload: build
	python -m twine upload dist/*
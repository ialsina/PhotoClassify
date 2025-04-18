.PHONY: test test-cov lint type-check clean format

# Python interpreter
PYTHON = python3

# Test coverage threshold
COVERAGE_THRESHOLD = 80

# Install development dependencies
install-dev:
	pip install -r requirements-dev.txt

# Run all tests
test:
	pytest tests/

# Run tests with coverage
test-cov:
	pytest --cov=photoclassify --cov-report=term-missing --cov-fail-under=$(COVERAGE_THRESHOLD) tests/

# Run linting
lint:
	flake8 photoclassify/ tests/
	isort --check-only photoclassify/ tests/
	black --check photoclassify/ tests/

# Run type checking
type-check:
	mypy photoclassify/ tests/

# Clean build artifacts
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name "*.egg" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".coverage" -exec rm -rf {} +
	find . -type d -name "htmlcov" -exec rm -rf {} +

# Format code
format:
	isort photoclassify/ tests/
	black photoclassify/ tests/

# Run all checks
check: lint type-check test-cov

# Default target
all: install-dev check 
lint:
	black --check
	flake8 alexflow/ tests/

test:
	pytest --cov=alexflow --cov-report=term-missing tests/

dev:
	pip install -r requirements.txt -c constraints.txt
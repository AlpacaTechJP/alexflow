lint:
	black --check alexflow/ tests/
	flake8 alexflow/ tests/

test:
	pytest --cov=alexflow --cov-report=term-missing tests/

black:
	black alexflow/ tests/ examples/

dev:
	pip install -r requirements.txt -c constraints.txt

clean:
	rm -rf dist/

release:
	python setup.py bdist_wheel
	twine upload dist/*
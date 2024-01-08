fmt:
	black .
	isort .
	mypy .

lint-check:
	black --check .
	isort --check .
	mypy .

test:
	python3 -m unittest
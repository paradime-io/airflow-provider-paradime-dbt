fmt:
	black paradime_dbt_provider/
	isort paradime_dbt_provider/
	mypy paradime_dbt_provider/

lint-check:
	black --check paradime_dbt_provider/
	isort --check paradime_dbt_provider/
	mypy paradime_dbt_provider/

test:
	python3 -m unittest
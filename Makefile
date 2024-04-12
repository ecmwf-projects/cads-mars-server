PROJECT := cads_mars_server
COV_REPORT := html

default: qa unit-tests type-check

qa:
	pre-commit run --all-files

unit-tests:
	python -m pytest -vv --cov=. --cov-report=$(COV_REPORT) --doctest-glob="*.md" --doctest-glob="*.rst"

type-check:
	python -m mypy .

# DO NOT EDIT ABOVE THIS LINE, ADD COMMANDS BELOW

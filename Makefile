PROJECT := cads_mars_server
CONDA := conda
CONDAFLAGS :=
COV_REPORT := html

default: qa unit-tests type-check

qa:
	pre-commit run --all-files

unit-tests:
	python -m pytest -vv --cov=. --cov-report=$(COV_REPORT) --doctest-glob="*.md" --doctest-glob="*.rst" -m "not requires_linux"

integration-tests:
	python -m pytest -vv --cov=. --cov-report=$(COV_REPORT) -m "requires_linux"

all-tests:
	python -m pytest -vv --cov=. --cov-report=$(COV_REPORT) --doctest-glob="*.md" --doctest-glob="*.rst"

docker-test:
	docker build -f Dockerfile.test -t $(PROJECT)-test .
	docker run --rm $(PROJECT)-test

type-check:
	python -m mypy .

conda-env-update:
	$(CONDA) install -y -c conda-forge conda-merge
	$(CONDA) run conda-merge environment.yml ci/environment-ci.yml > ci/combined-environment-ci.yml
	$(CONDA) env update $(CONDAFLAGS) -f ci/combined-environment-ci.yml

docker-build:
	docker build -t $(PROJECT) .

docker-run:
	docker run --rm -ti -v $(PWD):/srv $(PROJECT)

template-update:
	pre-commit run --all-files cruft -c .pre-commit-config-cruft.yaml

docs-build:
	cp README.md docs/. && cd docs && rm -fr _api && make clean && make html

# DO NOT EDIT ABOVE THIS LINE, ADD COMMANDS BELOW

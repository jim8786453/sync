
.DEFAULT_GOAL := help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
.PHONY: help

clean-pyc: ## Clean Python temporary files
	find . -name '*.pyc' -exec rm --force {} +
	find . -name '*.pyo' -exec rm --force {} +
	find . -name '*~' -exec rm --force  {} +

clean-build: ## Clean Python build directories
	rm --force --recursive build/
	rm --force --recursive dist/
	rm --force --recursive *.egg-info

clean: clean-pyc clean-build ## Clean all Python auto-generated files and virtual environments
	rm --force --recursive devenv

devenv: ## Build a Python virtual environment
	tox -e devenv

test: ## Run the unit test suite
	tox

run: ## Run a test server using Gunicorn
	. devenv/bin/activate && gunicorn -t 1000 sync.http.server:api --reload

docs: ## Build the api documentation
	. devenv/bin/activate && sphinx-apidoc -f -o docs/source/ sync

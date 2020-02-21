.PHONY: environment
## create virtual environment for butterfree
environment:
	@pyenv install -s 3.6.8
	@pyenv virtualenv 3.6.8 butterfree
	@pyenv local butterfree
	@PYTHONPATH=. python -m pip install --upgrade pip

.PHONY: requirements-test
## install test requirements
requirements-test:
	@PYTHONPATH=. python -m pip install -r requirements.test.txt

.PHONY: requirements-lint
## install lint requirements
requirements-lint:
	@PYTHONPATH=. python -m pip install -r requirements.lint.txt

.PHONY: dev-requirements
## install development requirements
dev-requirements:
	@PYTHONPATH=. python -m pip install -U -r requirements.dev.txt

.PHONY: minimum-requirements
## install prod requirements
minimum-requirements:
	@PYTHONPATH=. python -m pip install -U -r requirements.txt --extra-index-url https://quintoandar.github.io/python-package-server/

.PHONY: requirements
## install all requirements
requirements: requirements-test requirements-lint dev-requirements minimum-requirements

.PHONY: drone-install
drone-install:
	@pip install --upgrade pip
	@pip install cmake
	@python -m pip install -U -r requirements.test.txt -r requirements.lint.txt -r requirements.dev.txt -r requirements.txt -t ./pip/deps --cache-dir ./pip/cache

.PHONY: tests
## run all unit and integration tests with coverage report
tests:
	@python -m pytest -W ignore::DeprecationWarning --cov-config=.coveragerc --cov=butterfree --cov-report term --cov-report html:htmlcov --cov-report xml:coverage.xml tests
	@python -m coverage xml -i

.PHONY: unit-tests
## run unit tests with coverage report
unit-tests:
	@echo ""
	@echo "Unit Tests"
	@echo "=========="
	@echo ""
	@python -m pytest -W ignore::DeprecationWarning --cov-config=.coveragerc --cov-report term --cov-report html:unit-tests-cov --cov=butterfree --cov-fail-under=75 tests/unit


.PHONY: integration-tests
## run integration tests with coverage report
integration-tests:
	@echo ""
	@echo "Integration Tests"
	@echo "================="
	@echo ""
	@python -m pytest -W ignore::DeprecationWarning --cov-config=.coveragerc --cov-report term --cov-report xml:integration-tests-cov.xml --cov=butterfree --cov-fail-under=60 tests/integration

.PHONY: style-check
## run code style checks with black
style-check:
	@echo ""
	@echo "Code Style"
	@echo "=========="
	@echo ""
	@python -m black --check -t py36 --exclude="build/|buck-out/|dist/|_build/|pip/|\.pip/|\.git/|\.hg/|\.mypy_cache/|\.tox/|\.venv/" . && echo "\n\nSuccess" || (echo "\n\nFailure\n\nRun \"make black\" to apply style formatting to your code"; exit 1)

.PHONY: quality-check
## run code quality checks with flake8
quality-check:
	@echo ""
	@echo "Flake 8"
	@echo "======="
	@echo ""
	@python -m flake8 && echo "Success"
	@echo ""

.PHONY: checks
## run all code checks
checks: style-check quality-check

.PHONY: apply-style
## fix stylistic errors with black
apply-style:
	@python -m black -t py36 --exclude="build/|buck-out/|dist/|_build/|pip/|\.pip/|\.git/|\.hg/|\.mypy_cache/|\.tox/|\.venv/" .
	@python -m isort -rc butterfree/ tests/

.PHONY: clean
## clean unused artifacts
clean:
	@find ./ -type d -name 'dist' -exec rm -rf {} +;
	@find ./ -type d -name 'build' -exec rm -rf {} +;
	@find ./ -type d -name 'quintoandar_butterfree.egg-info' -exec rm -rf {} +;
	@find ./ -type d -name 'htmlcov' -exec rm -rf {} +;
	@find ./ -type d -name '.pytest_cache' -exec rm -rf {} +;
	@find ./ -type d -name 'spark-warehouse' -exec rm -rf {} +;
	@find ./ -type d -name 'metastore_db' -exec rm -rf {} +;
	@find ./ -type f -name 'coverage-badge.svg' -exec rm -f {} \;
	@find ./ -type f -name 'coverage.xml' -exec rm -f {} \;
	@find ./ -type f -name '.coverage*' -exec rm -f {} \;
	@find ./ -type f -name '*derby.log' -exec rm -f {} \;
	@find ./ -name '*.pyc' -exec rm -f {} \;
	@find ./ -name '*.pyo' -exec rm -f {} \;
	@find ./ -name '*~' -exec rm -f {} \;

.PHONY: version
## dump version into .version file and show
version:
	@grep __version__ setup.py | head -1 | cut -d \" -f2 | cut -d \' -f2 > .version
	@cat .version

.PHONY: commit-hash
## dump latest commit hash into .commit_has file and show
commit-hash:
	@git rev-parse HEAD > .commit_hash
	@cat .commit_hash

.PHONY: package-name
## dump package name into .package_name file and show
package-name:
	@grep __package_name__ setup.py | head -1 | cut -d \" -f2 | cut -d \' -f2 | sed 's/.*/&${build}/' > .package_name
	@cat .package_name

.PHONY: repository-url
## dump package repository url into .repository_ur file and show
repository-url:
	@grep __repository_url__ setup.py | head -1 | cut -d \" -f2 | cut -d \' -f2 > .repository_url

.PHONY: check-version
## checks whether actual version, in .version, already exists or not
check-version:
	@bash ./check_version.sh

.PHONY: package
## build quintoandar-butterfree package wheel
package:
	@PYTHONPATH=. python -m setup sdist bdist_wheel

.PHONY: publish
## publishes quintoandar-butterfree package wheel to quintoandar's private package server
publish:
	@bash ./publish.sh ${build}

.DEFAULT_GOAL := help

# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
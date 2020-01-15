.PHONY: environment
environment:
	@pyenv install -s 3.6.8
	@pyenv virtualenv 3.6.8 butterfree
	@pyenv local butterfree
	@PYTHONPATH=. python -m pip install --upgrade pip

.PHONY: requirements-test
requirements-test:
	@PYTHONPATH=. python -m pip install -r requirements.test.txt

.PHONY: requirements-lint
requirements-lint:
	@PYTHONPATH=. python -m pip install -r requirements.lint.txt

.PHONY: dev-requirements
dev-requirements:
	@PYTHONPATH=. python -m pip install -U -r requirements.dev.txt

.PHONY: minimum-requirements
minimum-requirements:
	@PYTHONPATH=. python -m pip install -U -r requirements.txt --extra-index-url https://quintoandar.github.io/python-package-server/

.PHONY: requirements
requirements: requirements-test requirements-lint dev-requirements minimum-requirements

.PHONY: drone-install
drone-install:
	@pip install --upgrade pip
	@python -m pip install -U -r requirements.test.txt -r requirements.lint.txt -r requirements.dev.txt -r requirements.txt -t ./pip/deps --cache-dir ./pip/cache

.PHONY: tests
tests:
	@python -m pytest -n=auto --cov-config=.coveragerc --cov=butterfree --cov-report term --cov-report html:htmlcov --cov-report xml:coverage.xml tests
	@python -m coverage xml -i

.PHONY: unit-tests
unit-tests:
	@echo ""
	@echo "Unit Tests"
	@echo "=========="
	@echo ""
	@python -m pytest -n auto --cov-config=.coveragerc --cov-report term --cov-report html:unit-tests-cov --cov=butterfree --cov-fail-under=75 tests/unit


.PHONY: integration-tests
integration-tests:
	@echo ""
	@echo "Integration Tests"
	@echo "================="
	@echo ""
	@python -m pytest -n auto --cov-config=.coveragerc --cov-report term --cov-report xml:integration-tests-cov.xml --cov=butterfree --cov-fail-under=60 tests/integration

.PHONY: style-check
style-check:
	@echo ""
	@echo "Code Style"
	@echo "=========="
	@echo ""
	@python -m black --check -t py36 --exclude="build/|buck-out/|dist/|_build/|pip/|\.pip/|\.git/|\.hg/|\.mypy_cache/|\.tox/|\.venv/" . && echo "\n\nSuccess" || echo "\n\nFailure\n\nRun \"make black\" to apply style formatting to your code"
	@echo ""

.PHONY: quality-check
quality-check:
	@echo ""
	@echo "Flake 8"
	@echo "======="
	@echo ""
	@python -m flake8 && echo "Success"
	@echo ""

.PHONY: checks
checks: style-check quality-check

.PHONY: apply-style
apply-style:
	@python -m black -t py36 --exclude="build/|buck-out/|dist/|_build/|pip/|\.pip/|\.git/|\.hg/|\.mypy_cache/|\.tox/|\.venv/" .
	@python -m isort -rc butterfree/ tests/

.PHONY: clean
clean:
	@find ./ -type d -name 'htmlcov' -exec rm -rf {} +;
	@find ./ -type d -name '.pytest_cache' -exec rm -rf {} +;
	@find ./ -type f -name 'coverage-badge.svg' -exec rm -f {} \;
	@find ./ -type f -name 'coverage.xml' -exec rm -f {} \;
	@find ./ -type f -name '.coverage*' -exec rm -f {} \;
	@find ./ -name '*.pyc' -exec rm -f {} \;
	@find ./ -name '*.pyo' -exec rm -f {} \;
	@find ./ -name '*~' -exec rm -f {} \;

.PHONY: version
version:
	@grep __version__ setup.py | head -1 | cut -d \" -f2 | cut -d \' -f2 > .version

.PHONY: package-name
package-name:
	@grep __package_name__ setup.py | head -1 | cut -d \" -f2 | cut -d \' -f2 > .package_name

.PHONY: repository-url
repository-url:
	@grep __repository_url__ setup.py | head -1 | cut -d \" -f2 | cut -d \' -f2 > .repository_url

.PHONY: check-version
check-version:
	@bash ./check_version.sh

.PHONY: package
package:
	@PYTHONPATH=. python -m setup sdist bdist_wheel

.PHONY: publish
publish:
	@bash ./publish.sh

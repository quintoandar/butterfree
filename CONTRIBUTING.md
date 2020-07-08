# Contributing to Butterfree

:tada: :clap:  First of all, thank you for wanting to contribute and be part of this project! :clap: :tada:

This document describes our guidelines for contributing to Butterfree and it's modules. The content will help with guides so that you can contribute more easily and so that our code maintains a high standard of quality. Probably not all possible cases will be covered in this document, so we hope you to use your best judgment, and feel free to help us enhance this document in a pull request. 

### Table of Contents
  * [Development Environment](#development-environment)
    + [Getting started](#getting-started)
      - [1. Clone the project:](#1-clone-the-project-)
      - [2. Setup the python environment for the project:](#2-setup-the-python-environment-for-the-project-)
        * [Errors](#errors)
      - [3. Install dependencies](#3-install-dependencies)
        * [Errors](#errors-1)
      - [Project](#project)
  * [Styleguides](#styleguides)
    + [Python Styleguide](#python-styleguide)
      - [Type Hint](#type-hint)
    + [Documentation Styleguide](#documentation-styleguide)
  * [Tests](#tests)
  * [GitFlow](#gitflow)
  * [Pull Requests](#pull-requests)


## Development Environment

At the bare minimum you'll need the following for your development
environment:

1. [Python 3.6.8](http://www.python.org/)


It is strongly recommended to also install and use [pyenv](https://github.com/pyenv/pyenv):

 - [pyenv-installer](https://github.com/pyenv/pyenv-installer)

This tool eases the burden of dealing with virtualenvs and having to activate and deactivate'em by hand. Once you run `pyenv local my-project-venv` the directory you're in will be bound to the `my-project-venv` virtual environment and then you will have
never to bother again activating the correct venv.

### Getting started

Run `make help` for more information on ready to use scripts.

#### 1. Clone the project:

```bash
    git clone git@github.com:quintoandar/butterfree.git
    cd butterfree
```

#### 2. Setup the python environment for the project:

```bash
make environment
```

If you need to configure your development environment in your IDE, notice
pyenv will store your python under
`~/.pyenv/versions/3.6.8 butterfree/bin/python`.

##### Errors

If you receive one error of missing OpenSSL to run the `pyenv install`, you can try to fix running:

```bash
sudo apt install -y libssl1.0-dev
```

#### 3. Install dependencies

```bash
make requirements
```

##### Errors

If you receive one error like this one:
```bash
 "import setuptools, tokenize;__file__='/tmp/pip-build-98gth33d/googleapis-common-protos/setup.py';
 .... 
 failed with error code 1 in /tmp/pip-build-98gth33d/googleapis-common-protos/
```
 
You can try to fix it running:

```bash
python -m pip install --upgrade pip setuptools wheel
```

#### Project

Library's content live under the [`quintoandar`](https://github.com/quintoandar/butterfree/tree/master/quintoandar) module, where you'll find [it's public API exposed](https://github.com/quintoandar/butterfree/tree/master/quintoandar/__init__.py).

## Styleguides

### Python Styleguide

TL;DR: Just run `make apply-style` before you commit. Check if everything is fine with `make checks`.

This project follows:
- [PEP8](https://www.python.org/dev/peps/pep-0008/) for code style.
- [PEP257](https://www.python.org/dev/peps/pep-0257/) with Google's docstring style ([example](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html#example-google)). 

The project uses some nice tooling to unifies style across the project's codebase and improve quality. S you don't need to worry about manually reviewing your style and imports, `black` and `isort` will automatically fix most of style inconsistency:

```bash
make apply-style
```

Additionally [Flake 8](http://flake8.pycqa.org/en/latest/) is used to check for other things such as unnecessary imports and code-complexity.

You can check Flake 8 and Black by running the following within the project root:

```bash
make checks
```

#### Type Hint

We use type hint in all of our methods arguments and in the return of the methods too. This way, our methods will have a very explicit declaration of is expected as inputs and outputs, making it easier for anyone to understand. Besides that, using type hints will help us with documentation. More information can be found in Python [docs](https://docs.python.org/3/library/typing.html)

Example:
```Python
def read(self, format: str, options: dict, stream: bool = False) -> DataFrame:
```

### Documentation Styleguide

#### Use [Google Style Documentation](https://github.com/google/styleguide/blob/gh-pages/pyguide.md#38-comments-and-docstrings).

From the common Python documentation styles that follow [PEP 257](https://www.python.org/dev/peps/pep-0257/) guides, we found Google's one the most human readable. Besides that, is totaly compatible with Sphinx with [Napoleon](https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html) plugin.

You can easily configure PyCharm to use this style in "Python Integrated Tools":

![](https://i.imgur.com/sv0hcBA.png)

There is **no need to write about the types** of arguments or returns of a method since we decide to use type hints instead. Using [this](https://pypi.org/project/sphinx-autodoc-typehints/) plugin metadata about types in documentation can be easily generated by Sphinx.

Example of class documentation:
```Python
class CassandraConfig(AbstractWriteConfig):
    """Configuration for Spark to connect on Cassandra DB.

    References can be found
    [here](https://docs.databricks.com/data/data-sources/cassandra.html).

    Attributes:
        mode: write mode for Spark.
        format_: write format for Spark.
        keyspace:  Cassandra DB keyspace to write data.
        username: username to use in connection.
        password: password to use in connection.
        host: host to use in connection.
    """
```

More examples of documentation for classes, methods, properties and more [here](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html).

## Tests

TL;DR: Just run `make tests` to check if your code is fine.

This project is thoroughly tested as of the time of this writing. Unit tests rely under the [test module](https://github.com/quintoandar/butterfree/tree/master/tests/unit) and integration tests, under the [integration_test module](https://github.com/quintoandar/butterfree/tree/master/tests/integration). [pytest](https://docs.pytest.org/en/latest/) is used to write all of this project's tests. 

**Before opening a PR, check if all your new code is covered with tests.**

You can run unit tests by issuing make at the project's root:
```bash
make unit-tests
```

You can run integration tests in the same fashion:
```bash
make integration-test
```

Style check is available through make too:
```bash
make style-check
make quality-check
```

You can run unit tests, integration tests and style check in a single batch:
```bash
make tests
```

To test all notebook examples under `examples/` folder, you can run:
```bash
make test-examples
```

## GitFlow
Please **follow the guidelines** described [here](GITFLOW.md).

## Pull Requests
TBD

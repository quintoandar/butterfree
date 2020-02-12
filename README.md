## Butterfree Python Library

_This library is part of the QuintoAndar-specific libraries.

This repository contains ETL scripts defining the feature sets. Our current architecture is the following:

![](https://i.imgur.com/38fhHJI.png)

```python
from butterfree import my_lib

def foo(arg):
    return my_lib.awesome_function(arg)  # change me
```

This library supports Python version 3.6+ and meant for COMPLETE ME!

## Installing

[QuintoAndar's Python Package Index](https://quintoandar.github.io/python-package-server/)
hosts reference to a pip-installable module of this library, using it is
as straightforward as including it on your project's requirements.

```bash
pip install quintoandar-butterfree --extra-index-url https://quintoandar.github.io/python-package-server/
```

Or after listing `quintoandar-butterfree` in your
`requirements.txt` file:

```bash
pip install -r requirements.txt --extra-index-url https://quintoandar.github.io/python-package-server/
```

This will expose `my_lib` under `butterfree` module:

```python
from butterfree import my_lib

def foo():
    bar = my_lib.cool_method()
```

## Development Environment

At the bare minimum you'll need the following for your development
environment:

1. [Python 3.6.8](http://www.python.org/)


It is strongly recommended to also install and use [pyenv](https://github.com/pyenv/pyenv):

 - [pyenv-installer](https://github.com/pyenv/pyenv-installer)

This tool eases the burden of dealing with virtualenvs and having to activate and
deactivate'em by hand. Once you run `pyenv local my-project-venv` the directory you're
in will be bound to the `my-project-venv` virtual environment and then you will have
never to bother again activating the correct venv.

## Getting started

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

## Development

Library's content live under the [`quintoandar`](https://github.com/quintoandar/butterfree/tree/master/quintoandar)
module, where you'll find [it's public API exposed](https://github.com/quintoandar/butterfree/tree/master/quintoandar/__init__.py).


### Tests

TL;DR: Just run `make tests` to check if your code is fine.

This project is thoroughly tested as of the time of this writing. Unit tests
rely under the [test module](https://github.com/quintoandar/butterfree/tree/master/tests/unit)
and integration tests, under the [integration_test module](https://github.com/quintoandar/butterfree/tree/master/tests/integration).

[pytest](https://docs.pytest.org/en/latest/)
is used to write all of this project's tests.

You can run unit tests by issuing make at the project's root:
```bash
make unit-tests
```

You can run
integration tests in the same fashion:
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

### Code Style, PEP8 & Formatting

TL;DR: Just run `make apply-style` before you commit.
Check if everything is fine with `make checks`.

This project follows:
- [PEP8](https://www.python.org/dev/peps/pep-0008/) for code style.
- [PEP257](https://www.python.org/dev/peps/pep-0257/) with Google's docsting
style ([example](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html#example-google)).
using some nice tooling to unifies style across the project's codebase and
improve quality.

No need to worry about manually reviewing your style and imports,
`black` and `isort` will automatically fix most of style inconsistency:

```bash
make apply-style
```

Additionally [Flake 8](http://flake8.pycqa.org/en/latest/) is used to
check for other things such as unnecessary imports and code-complexity.

You can check Flake 8 and Black by running the following within the project root:

```bash
make checks
```

## Release

TL;DR: Merge your Pull Request to `master` and everything will be taken
care of. **Just make sure you update the lib version in [`setup.py`](https://github.com/quintoandar/butterfree/tree/master/setup.py)**.

Drone pipeline will generate a new tag whenever there is a push to
`master` titled with the version number specified by [`__version__` in
`setup.py`](https://github.com/quintoandar/butterfree/tree/master/setup.py).

The link to this tag will be automatically pushed to this module's index
in [QuintoAndar's Python Package Index](https://quintoandar.github.io/python-package-server/tree/master/quintoandar-butterfree/index.html)
and will be ready to consume.

## Contributing

Any contributions are welcome! Feel free to open Pull Requests and
posting them to [our
**#data-products-reviews** slack channel](https://quintoandar.slack.com/messages/data-products-reviews/).

Please **follow the guidelines** described [here](https://github.com/quintoandar/butterfree/tree/master/CONTRIBUTING.md)

from setuptools import find_packages, setup

__package_name__ = "quintoandar-butterfree"
__version__ = "0.1.0"
__repository_url__ = "https://github.com/quintoandar/butterfree-python"

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name=__package_name__,
    version=__version__,
    url=__repository_url__,
    packages=find_packages(
        exclude=["tests", "pipenv", "env", "examples", "htmlcov", ".pytest_cache"]
    ),
    license="Copyright",
    author="QuintoAndar",
    description="This repository contains ETL scripts defining the feature sets",
    keywords="feature store feature sets ETL",
    install_requires=requirements,
)

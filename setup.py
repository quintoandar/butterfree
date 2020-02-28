from setuptools import find_packages, setup

__package_name__ = "quintoandar-butterfree"
__version__ = "0.3.0"
__repository_url__ = "https://github.com/quintoandar/butterfree"

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name=__package_name__,
    version=__version__,
    url=__repository_url__,
    packages=find_packages(
        exclude=(
            "tests",
            "tests.*",
            "pipenv",
            "env",
            "examples",
            "htmlcov",
            ".pytest_cache",
        )
    ),
    license="Copyright",
    author="QuintoAndar",
    description="This repository contains ETL scripts defining the feature sets",
    keywords="feature store sets ETL",
    install_requires=requirements,
    extras_require={"h3": ["cmake==3.16.3", "h3==3.4.2"]},
)

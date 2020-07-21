from setuptools import find_packages, setup

__package_name__ = "butterfree"
__version__ = "1.0.0"
__repository_url__ = "https://github.com/quintoandar/butterfree"

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

with open("README.md") as f:
    long_description = f.read()

setup(
    name=__package_name__,
    description="A tool for building feature stores.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="feature store sets ETL",
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
    install_requires=requirements,
    extras_require={"h3": ["cmake==3.16.3", "h3==3.4.2"]},
)

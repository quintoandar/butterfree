## Butterfree
_A tool for building feature stores. Transform your raw data into beautiful features._

[![Release](https://img.shields.io/github/v/release/quintoandar/butterfree)]((https://pypi.org/project/butterfree/))
![Python Version](https://img.shields.io/badge/python-3.7%20%7C%203.8-brightgreen.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

| Source    | Downloads                                                                                                                       | Page                                                 | Installation Command                       |
|-----------|---------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|--------------------------------------------|
| **PyPi**  | [![PyPi Downloads](https://pepy.tech/badge/butterfree)](https://pypi.org/project/butterfree/)                      | [Link](https://pypi.org/project/butterfree/)        | `pip install butterfree `                  |

### Build status
| Develop                                                                     | Stable                                                                            | Documentation                                                                                                                                           | Sonar                                                                                                                                                                                    |
|-----------------------------------------------------------------------------|-----------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ![Test](https://github.com/quintoandar/butterfree/workflows/Test/badge.svg) | ![Publish](https://github.com/quintoandar/butterfree/workflows/Publish/badge.svg) | [![Documentation Status](https://readthedocs.org/projects/butterfree/badge/?version=latest)](https://butterfree.readthedocs.io/en/latest/?badge=latest) | [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=quintoandar_butterfree&metric=alert_status)](https://sonarcloud.io/dashboard?id=quintoandar_butterfree) |


Made with :heart: by the **MLOps** team from [QuintoAndar](https://github.com/quintoandar/)

This library supports Python version 3.7+ and meant to provide tools for building ETL pipelines for Feature Stores using [Apache Spark](https://spark.apache.org/).

The library is centered on the following concetps:
- **ETL**: central framework to create data pipelines. Spark-based **Extract**, **Transform** and **Load** modules ready to use.
- **Declarative Feature Engineering**: care about **what** you want to compute and **not how** to code it.
- **Feature Store Modeling**: the library easily provides everything you need to process and load data to your Feature Store.

To understand the main concepts of Feature Store modeling and library main features you can check [Butterfree's Documentation](https://butterfree.readthedocs.io/en/latest/home.html), which is hosted by Read the Docs.

To learn how to use Butterfree in practice, see [Butterfree's notebook examples](https://github.com/quintoandar/butterfree/tree/master/examples)  

## Requirements and Installation
Butterfree depends on **Python 3.7+** and it is **Spark 3.0 ready** :heavy_check_mark:

[Python Package Index](https://quintoandar.github.io/python-package-server/) hosts reference to a pip-installable module of this library, using it is as straightforward as including it on your project's requirements.

```bash
pip install butterfree
```

Or after listing `butterfree` in your `requirements.txt` file:

```bash
pip install -r requirements.txt
```

Dev Package are available for testing using the <version>.devN versions of the Butterfree on PyPi.

## License
[Apache License 2.0](https://github.com/quintoandar/butterfree/blob/staging/LICENSE)

## Contributing
All contributions are welcome! Feel free to open Pull Requests. Check the development and contributing **guidelines** described [here](CONTRIBUTING.md).

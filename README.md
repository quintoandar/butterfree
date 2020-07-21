## Butterfree
Made with :heart: by the **MLOps** team from [QuintoAndar](https://github.com/quintoandar/)

This library supports Python version 3.6+ and meant to provide tools for building ETL pipelines for Feature Stores using [Apache Spark](https://spark.apache.org/).

The library is centered on the following concetps:
- **ETL**: central framework to create data pipelines. Spark-based **Extract**, **Transform** and **Load** modules ready to use.
- **Declarative Feature Engineering**: care about **what** you want to compute and **not how** to code it.
- **Feature Store Modeling**: the library easily provides everything you need to process and load data to your Feature Store.

To understand the main concepts of Feature Store modeling and library main features you can check [Butterfree's Wiki](https://github.com/quintoandar/butterfree/wiki).

To learn how to use Butterfree in practice, see [Butterfree's notebook examples](https://github.com/quintoandar/butterfree/tree/master/examples)  

## Requirements and Installation
Butterfree depends on **Python 3.6+** and it is **Spark 3.0 ready** :heavy_check_mark:

[Python Package Index](https://quintoandar.github.io/python-package-server/) hosts reference to a pip-installable module of this library, using it is as straightforward as including it on your project's requirements.

```bash
pip install butterfree
```

Or after listing `butterfree` in your `requirements.txt` file:

```bash
pip install -r requirements.txt
```

You may also have access to our preview build (unstable) by installing from `staging` branch:

```bash
pip install git+https://github.com/quintoandar/butterfree.git@staging
```

## Documentation
The official documentation is hosted on [Read the Docs](https://butterfree.readthedocs.io/en/latest/home.html)

## License
[Apache License 2.0](https://github.com/quintoandar/butterfree/blob/staging/LICENSE)

## Contributing
All contributions are welcome! Feel free to open Pull Requests. Check the development and contributing **guidelines** described [here](CONTRIBUTING.md).

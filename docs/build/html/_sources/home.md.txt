# Welcome to the Butterfree Docs!

The main idea is for this repository to be a set of tools for easing [ETLs](https://en.wikipedia.org/wiki/Extract,_transform,_load). The idea is using Butterfree to upload data to a Feature Store, so data can be provided to your machine learning algorithms.

## Table of Contents
- [What is going on here?](#what-is-going-on-here)
- [Service Architecture](#services-architecture)
- [Extract](#extract)
- [Transform](#transform)
- [Load](#load)
- [Streaming](#streaming)
- [Setup Configuration](#setup-configuration)

## What is going on here

Besides introducing Butterfree itself, it's necessary to define some concepts that will be presented throughout this Docs.

The feature store is where features for machine learning models and pipelines are stored. A feature is an individual property or characteristic of a data-sample, such as the height of a person, the area of a house or an aggregated feature as the average prices of houses seen by a user within the last day. A feature set can be thought of as a set of features. Finally, an entity is a unity representation of a specific business context.

This repository holds all scripts that will extract data from necessary sources (S3 and Kafka, for instance), transform all of this raw data into feature sets and, finally, upload these results in a feature store.

Scripts use Python and [Apache's Spark](https://spark.apache.org/).

It's important to highlight that a feature store incurs some challenging concerns, such as:
* Dealing with big volumes of data;
* Retroactively computing new features;
* Loading historical and online data for consumption;
* Keeping it simple to add new features and functionality;
* Making it easy to share code.

## Service Architecture

Regarding this architecture, some requisites can be emphasized, for instance:
* We currently support [Amazon S3](https://aws.amazon.com/s3/) and [Apache Kafka](https://kafka.apache.org/) as data sources;
* A hive metastore is required within your Spark session;
* An [Apache Cassandra](https://cassandra.apache.org/) database should be up and running.

![](https://i.imgur.com/Nu3Dwet.png)

Some important core concepts regarding this service are features, feature sets and entities, discussed previously. Besides, it's possible to draw attention to other two major ideas:
* Historical Feature Store: all features are calculated over time and stored at S3;
* Online Feature Store: hot/latest data stored at a low latency data storage, such as Apache Cassandra.

## Extract

Basically, the extract step is performed with a ```Source``` object, by defining the desired data sources. You can learn more about it in the [Extract Session](extract.md).

## Transform

The transform step is defined within a ```FeatureSet```, by explicitly defining the desired transformations. More information about the transformations can be found at the [Transform Session](transform.md).

## Load

It's the last step of the ETL process, and it's defined by a ```Sink``` object. Please, refer to the [Sink Session](load.md) to know more. 

## Streaming

We also support streaming pipelines in Butterfree. More information is available at the [Streaming Session](stream.md). 


## Setup Configuration

Some configurations are needed to run your ETL pipelines. Detailed information is provided at the [Configuration Section](configuration.md)

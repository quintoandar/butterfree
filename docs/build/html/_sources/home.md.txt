# Welcome to the Butterfree Docs!

The main idea is for this repository to be a set of tools for easing [ETLs](https://en.wikipedia.org/wiki/Extract,_transform,_load). The idea is using Butterfree to upload data to a Feature Store, so data can be provided to your machine learning algorithms.

## Table of Contents
- [What is going on here?](#what-is-going-on-here)
- [Service Architecture](#services-architecture)
- [Extract](#extract)
- [Transform](#transform)
- [Load](#load)

## What is going on here

Besides introducing Butterfree itself, it's necessary to define some concepts that will be presented throughout this Doc.

The feature store is where features for machine learning models and pipelines are stored. A feature is an individual property or characteristic of a data-sample, such as the height of a person, the area of a house or an aggregated feature as the average prices of houses seen by a user within the last day. Finally, a feature set can be thought as a set of features.

This repository holds all scripts that will extract data from necessary sources (S3 and Kafka, for instance), transform all of this raw data into feature sets and, finally, upload these results in a feature store.

Scripts use Python and [Apache's Spark](https://spark.apache.org/).

## Services Architecture

![](https://i.imgur.com/Nu3Dwet.png)

## Extract

Basically, the extract step is performed with a ```Source``` object, by defining the desired data sources. You can learn more about it in the [Extract Session](extract.md).

## Transform

The transform step is defined within a ```FeatureSet```, by explicit defining the desired transformations. More information about the transformations can be found at the [Transform Session](transform.md).

## Load

It's the last step of the ETL process, and it's defined by a ```Sink``` object. Please, refer to the [Sink Session](load.md) to know more. 
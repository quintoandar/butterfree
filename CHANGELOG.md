# Changelog
All notable changes to this project will be documented in this file.

Preferably use **Added**, **Changed**, **Removed** and **Fixed** topics in each release or unreleased log for a better organization.

## Unreleased
## [0.10.3](https://github.com/quintoandar/butterfree/releases/tag/0.10.3)
### Fixed
* [MLOP-370] Bug in streaming feature sets ([#178](https://github.com/quintoandar/butterfree/pull/178))
* [MLOP-342] Fix docstrings in the modules for documentation ([#173](https://github.com/quintoandar/butterfree/pull/173))
* Fix PyYAML version ([#172](https://github.com/quintoandar/butterfree/pull/172))

## [0.10.2](https://github.com/quintoandar/butterfree/releases/tag/0.10.2)
### Added
* [MLOP-316] Test Notebook Examples Makefile Command ([#165](https://github.com/quintoandar/butterfree/pull/165))
* [MLOP-254] Create notebook with aggregated feature set ([#164](https://github.com/quintoandar/butterfree/pull/164))
* [MLOP-255] Stream Notebook Example ([#167](https://github.com/quintoandar/butterfree/pull/167))
* [MLOP-325] Enable stream in FileReader and OnlineFeatureStoreWriter in Debug Mode ([#166](https://github.com/quintoandar/butterfree/pull/166))

### Fixed
* Fix PyYAML version. ([#169](https://github.com/quintoandar/butterfree/pull/169))

## [0.10.1](https://github.com/quintoandar/butterfree/releases/tag/0.10.1)
### Added
* [MLOP-253] Create notebook with spark functions transform and windows ([#160](https://github.com/quintoandar/butterfree/pull/160))

### Fixed
* Fix cast to ArrayType ([#161](https://github.com/quintoandar/butterfree/pull/161))

## [0.10.1](https://github.com/quintoandar/butterfree/releases/tag/0.10.1)
### Added
* [MLOP-253] Create notebook with spark functions transform and windows ([#160](https://github.com/quintoandar/butterfree/pull/160))

### Fixed
* Fix cast to ArrayType ([#161](https://github.com/quintoandar/butterfree/pull/161))

## [0.10.0](https://github.com/quintoandar/butterfree/releases/tag/0.10.0)
### Added
* [MLOP-325] Debug Mode on Writers ([#155](https://github.com/quintoandar/butterfree/pull/155))
* [MLOP-256] Create a checklist for Butterfree PRs ([#153](https://github.com/quintoandar/butterfree/pull/153))
* [MLOP-252] Butterfree Examples - Simple Feature Set ([#157](https://github.com/quintoandar/butterfree/pull/157))

### Changed
* [MLOP-291] Associate Multiple Transforms to their Correspondents Types ([#154](https://github.com/quintoandar/butterfree/pull/154))

### Fixed
* Fix cassandra client ([#156](https://github.com/quintoandar/butterfree/pull/156))

## [0.9.2](https://github.com/quintoandar/butterfree/releases/tag/0.9.2)
### Added
* Add most_frequent_set to allowed agg ([#150](https://github.com/quintoandar/butterfree/pull/150))

## [0.9.1](https://github.com/quintoandar/butterfree/releases/tag/0.9.1)
### Added
* [MLOP-280] Create new most frequent set aggregation ([#145](https://github.com/quintoandar/butterfree/pull/145))

### Changed
* Optimize Aggregated Feature Sets with Repartition ([#147](https://github.com/quintoandar/butterfree/pull/147))
* [MLOP-281] Fix target dataframe in test_aggregated_feature_set ([#146](https://github.com/quintoandar/butterfree/pull/146))

## [0.9.0](https://github.com/quintoandar/butterfree/releases/tag/0.9.0)
### Added
* [MLOP-191] AggregatedTransform with filter option to use subset during aggregation ([#139](https://github.com/quintoandar/butterfree/pull/139))
* [MLOP-190] AggregateTransform with distinct_on option to de-duplicate auditable/historical tables ([#138](https://github.com/quintoandar/butterfree/pull/138))

### Changed
* [MLOP-248] HistoricaFeatureStoreWriter validation count threshold ([#140](https://github.com/quintoandar/butterfree/pull/140))

## [0.8.0](https://github.com/quintoandar/butterfree/releases/tag/0.8.0)
### Changed
* [PROPOSAL] Optimizing rolling window aggregations ([#134](https://github.com/quintoandar/butterfree/pull/134))

## [0.7.1](https://github.com/quintoandar/butterfree/releases/tag/0.7.1)
### Added
* [MLOP-225] KeyFeature need dtype to be a required arg ([#126](https://github.com/quintoandar/butterfree/pull/126))

### Changed
* [MLOP-229] Revert cross join changes ([#130](https://github.com/quintoandar/butterfree/pull/130))
* Update kafak consumer config ([#127](https://github.com/quintoandar/butterfree/pull/127))

### Fixed
* [MLOP-231] Method output_columns supports Pivot Aggregated ([#128](https://github.com/quintoandar/butterfree/pull/128))
* Fix dtype on Keyfeature. ([#129](https://github.com/quintoandar/butterfree/pull/129))

## [0.7.0](https://github.com/quintoandar/butterfree/releases/tag/0.7.0)
### Added
* [MLOP-140] Make OFSW get feature set schema ([#112](https://github.com/quintoandar/butterfree/pull/112))
* [MLOP-188] Pivot within Aggregated Transform ([#115](https://github.com/quintoandar/butterfree/pull/115))
* [MLOP-171] Make entities available through Cassandra ([#110](https://github.com/quintoandar/butterfree/pull/110))
* [MLOP-204] Add repartion method after source is built ([#111](https://github.com/quintoandar/butterfree/pull/111))

### Changed
* [MLOP-228] Remove cross join from AggregatedFeatureSet ([#123](https://github.com/quintoandar/butterfree/pull/123))

### Fixed
* [MLOP-227] FeatureSetPipeline Construct Method Fix ([#122](https://github.com/quintoandar/butterfree/pull/122))

## [0.6.0](https://github.com/quintoandar/butterfree/releases/tag/0.6.0)
### Added
* [MLOP-209] with_stack on H3HashTransform ([#114](https://github.com/quintoandar/butterfree/pull/114))
* [MLOP-141] Create client cassandra on butterfree ([#109](https://github.com/quintoandar/butterfree/pull/109))
* [MLOP-185] StackTransform ([#105](https://github.com/quintoandar/butterfree/pull/105))
* [MLOP-176] MostFrequent aggregation ([#104](https://github.com/quintoandar/butterfree/pull/104))

### Changed
* [MLOP-202] Refactoring Agg - Create AggregatedFeatureSet and refactor AggregatedTransform ([#108](https://github.com/quintoandar/butterfree/pull/108))
* [MLOP-137] Make data_type argument mandatory ([#107](https://github.com/quintoandar/butterfree/pull/107))
* [MLOP-136] Improve DataType with supported spark/cassandra types ([#106](https://github.com/quintoandar/butterfree/pull/106))
* [MLOP-201] Refactoring Agg - Create FrameBoundaries, Window and SparkFunctionTransform ([#103](https://github.com/quintoandar/butterfree/pull/103))
* [MLOP-168] HistoricalFeatureStoreWriter Create Partitions Tests and Refactoring ([#102](https://github.com/quintoandar/butterfree/pull/102))

### Fixed
* Fix Types ([#113](https://github.com/quintoandar/butterfree/pull/113))
* [MLOP-221] H3 and AggregatedFeatureSet bug fix ([#119](https://github.com/quintoandar/butterfree/pull/119))

## [0.5.0](https://github.com/quintoandar/butterfree/releases/tag/0.5.0)
### Added
* [MLOP-177] Aggregate by rows (rowsBetween window) ([#86](https://github.com/quintoandar/butterfree/pull/86))
* [MLOP-186] New aggregations to ALLOWED_AGGREGATIONS enum ([#88](https://github.com/quintoandar/butterfree/pull/88))
* [MLOP-181] FileReader update docstring with schema usage ([#93](https://github.com/quintoandar/butterfree/pull/93))
* [MLOP-189] Replace (map values) as a pre_processing function ([#92](https://github.com/quintoandar/butterfree/pull/92))

## [0.4.0](https://github.com/quintoandar/butterfree/releases/tag/0.4.0)
### Added
* [MLOP-169] Enable Stream Pipelines in Butterfree ([#81](https://github.com/quintoandar/butterfree/pull/81))
* Safeguard on date conversion ([#87](https://github.com/quintoandar/butterfree/pull/87))

## [0.3.3](https://github.com/quintoandar/butterfree/releases/tag/0.3.3)
* Collect set aggregation ([#80](https://github.com/quintoandar/butterfree/pull/80))
* Add drone step for automatic releasing ([#82](https://github.com/quintoandar/butterfree/pull/82))
* Remove OnlineFeatureStoreWriter validations ([#83](https://github.com/quintoandar/butterfree/pull/83))


## [0.3.2](https://github.com/quintoandar/butterfree/releases/tag/0.3.2)
* [MLOP-167] Fix Repartition Method ([#76](https://github.com/quintoandar/butterfree/pull/76))
* [MLOP-152] Tuning dataframe generated by FeatureSet ([#75](https://github.com/quintoandar/butterfree/pull/75))
* [MLOP-151] Refactor FeatureSet and Pipeline - Agg Feature bug ([#74](https://github.com/quintoandar/butterfree/pull/74))

## [0.3.1](https://github.com/quintoandar/butterfree/releases/tag/0.3.1)

### Fixed
* [MLOP-157] Ambiguous Name Bug on Feature Set ([#71](https://github.com/quintoandar/butterfree/pull/71))

## [0.3.0](https://github.com/quintoandar/butterfree/releases/tag/0.3.0)

### Added
* [MLOP-153] Butterfree testing module - Compare DataFrames ([#68](https://github.com/quintoandar/butterfree/pull/68))

## [0.2.0](https://github.com/quintoandar/butterfree/releases/tag/0.2.0)

### Added
* Flow with Staging Branch configuration ([#61](https://github.com/quintoandar/butterfree/pull/61))
* [MLOP-91] Usage examples in docstrings for load step ([#57](https://github.com/quintoandar/butterfree/pull/57))
* [MLOP-126] Timestamp conversions from long in ms (ure.timestamp) ([#56](https://github.com/quintoandar/butterfree/pull/56))
* [MLOP-66] Usage examples in docstrings for extract step ([#53](https://github.com/quintoandar/butterfree/pull/53))
* [MLOP-90] Usage examples in docstrings for transform step ([#52](https://github.com/quintoandar/butterfree/pull/52))

### Changed
* [MLOP-149] Use pytest-spark fixtures ([#64](https://github.com/quintoandar/butterfree/pull/64))
* [MLOP-123] Standardize namespaces on Butterfree ([#63](https://github.com/quintoandar/butterfree/pull/63))
* Split static and dynamic configurations ([#58](https://github.com/quintoandar/butterfree/pull/58))

### Fixed
* [MLOP-133] Fix pivoting without traceback ([#65](https://github.com/quintoandar/butterfree/pull/65))
* [MLOP-143] Fix Bugs for HouseMain FeatureSet ([#62](https://github.com/quintoandar/butterfree/pull/62))

## [0.1.0](https://github.com/quintoandar/butterfree/releases/tag/0.1.0)
* First modules and entities of butterfree package.
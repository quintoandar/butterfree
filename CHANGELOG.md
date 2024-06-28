# Changelog
All notable changes to this project will be documented in this file.

Preferably use **Added**, **Changed**, **Removed** and **Fixed** topics in each release or unreleased log for a better organization.

## [Unreleased]

## [1.3.5](https://github.com/quintoandar/butterfree/releases/tag/1.3.5)
* Auto create feature sets ([#368](https://github.com/quintoandar/butterfree/pull/368))

## [1.2.4](https://github.com/quintoandar/butterfree/releases/tag/1.2.4)
* Auto create feature sets ([#351](https://github.com/quintoandar/butterfree/pull/351))

## [1.2.3](https://github.com/quintoandar/butterfree/releases/tag/1.2.3)
* Optional params ([#347](https://github.com/quintoandar/butterfree/pull/347))

### Changed
* Optional row count validation ([#340](https://github.com/quintoandar/butterfree/pull/340))

## [1.2.2](https://github.com/quintoandar/butterfree/releases/tag/1.2.2)

### Changed
* Optional row count validation ([#284](https://github.com/quintoandar/butterfree/pull/284))
* Bump several libs versions ([#333](https://github.com/quintoandar/butterfree/pull/333))

## [1.2.1](https://github.com/quintoandar/butterfree/releases/tag/1.2.1)

### Changed
* Update README.md ([#331](https://github.com/quintoandar/butterfree/pull/331))
* Update Github Actions Workflow runner ([#332](https://github.com/quintoandar/butterfree/pull/332))
* Delete sphinx version. ([#334](https://github.com/quintoandar/butterfree/pull/334))

### Fixed
* Add the missing link for H3 geohash ([#330](https://github.com/quintoandar/butterfree/pull/330))

## [1.2.0](https://github.com/quintoandar/butterfree/releases/tag/1.2.0)

### Added
* [MLOP-636] Create migration classes ([#282](https://github.com/quintoandar/butterfree/pull/282))
* [MLOP-635] Rebase Incremental Job/Interval Run branch for test on selected feature sets ([#278](https://github.com/quintoandar/butterfree/pull/278))
* Allow slide selection ([#293](https://github.com/quintoandar/butterfree/pull/293))
* [MLOP-637] Implement diff method ([#292](https://github.com/quintoandar/butterfree/pull/292))
* [MLOP-640] Create CLI with migrate command ([#298](https://github.com/quintoandar/butterfree/pull/298))
* [MLOP-645] Implement query method, cassandra ([#291](https://github.com/quintoandar/butterfree/pull/291))
* [MLOP-671] Implement get_schema on Spark client ([#301](https://github.com/quintoandar/butterfree/pull/301))
* [MLOP-648] Implement query method, metastore ([#294](https://github.com/quintoandar/butterfree/pull/294))
* [MLOP-647] / [MLOP-646] Apply migrations ([#300](https://github.com/quintoandar/butterfree/pull/300))
* [MLOP-639] Track logs in S3 ([#306](https://github.com/quintoandar/butterfree/pull/306))
* [MLOP-702] Debug mode for Automate Migration ([#322](https://github.com/quintoandar/butterfree/pull/322))

### Changed
* Keep milliseconds when using 'from_ms' argument in timestamp feature ([#284](https://github.com/quintoandar/butterfree/pull/284))
* Read and write consistency level options ([#309](https://github.com/quintoandar/butterfree/pull/309))
* [MLOP-691] Include step to add partition to SparkMetastore during writing of Butterfree ([#327](https://github.com/quintoandar/butterfree/pull/327))

### Fixed
* [BUG] Apply create_partitions to historical validate ([#303](https://github.com/quintoandar/butterfree/pull/303))
* [BUG] Fix key path for validate read ([#304](https://github.com/quintoandar/butterfree/pull/304))
* [FIX] Add Partition types for Metastore ([#305](https://github.com/quintoandar/butterfree/pull/305))
* Change solution for tracking logs ([#308](https://github.com/quintoandar/butterfree/pull/308))
* [BUG] Fix Cassandra Connect Session ([#316](https://github.com/quintoandar/butterfree/pull/316))
* Fix method to generate agg feature name. ([#326](https://github.com/quintoandar/butterfree/pull/326))

## [1.1.3](https://github.com/quintoandar/butterfree/releases/tag/1.1.3)
### Added
* [MLOP-599] Apply mypy to ButterFree ([#273](https://github.com/quintoandar/butterfree/pull/273))

### Changed
* [MLOP-634] Butterfree dev workflow, set triggers for branches staging and master ([#280](https://github.com/quintoandar/butterfree/pull/280))
* Keep milliseconds when using 'from_ms' argument in timestamp feature ([#284](https://github.com/quintoandar/butterfree/pull/284))
* [MLOP-633] Butterfree dev workflow, update documentation ([#281](https://github.com/quintoandar/butterfree/commit/74278986a49f1825beee0fd8df65a585764e5524))
* [MLOP-632] Butterfree dev workflow, automate release description ([#279](https://github.com/quintoandar/butterfree/commit/245eaa594846166972241b03fddc61ee5117b1f7))

### Fixed
* Change trigger for pipeline staging ([#287](https://github.com/quintoandar/butterfree/pull/287))

## [1.1.2](https://github.com/quintoandar/butterfree/releases/tag/1.1.2)
### Fixed
* [HOTFIX] Add both cache and count back to Butterfree ([#274](https://github.com/quintoandar/butterfree/pull/274))
* [MLOP-606] Change docker image in Github Actions Pipeline ([#275](https://github.com/quintoandar/butterfree/pull/275))
* FIX Read the Docs build ([#272](https://github.com/quintoandar/butterfree/pull/272))
* [BUG] Fix style ([#271](https://github.com/quintoandar/butterfree/pull/271))
* [MLOP-594] Remove from_column in some transforms ([#270](https://github.com/quintoandar/butterfree/pull/270))
* [MLOP-536] Rename S3 config to Metastore config ([#269](https://github.com/quintoandar/butterfree/pull/269))

## [1.1.1](https://github.com/quintoandar/butterfree/releases/tag/1.2.0)
### Added
* [MLOP-590] Adapt KafkaConfig to receive a custom topic name ([#266](https://github.com/quintoandar/butterfree/pull/266))

## [1.1.0](https://github.com/quintoandar/butterfree/releases/tag/1.1.0)
### Added
* [MLOP-563] Enable Butterfree to write to Kafka ([#258](https://github.com/quintoandar/butterfree/pull/258))

### Changed
* Update README ([#257](https://github.com/quintoandar/butterfree/pull/257))

### Fixed
* Fix Butterfree's workflow ([#262](https://github.com/quintoandar/butterfree/pull/262))
* [FIX] Downgrade Python Version in Pyenv ([#227](https://github.com/quintoandar/butterfree/pull/227))
* [FIX] Fix docs ([#229](https://github.com/quintoandar/butterfree/pull/229))
* [FIX] Fix Docs - Add more dependencies ([#230](https://github.com/quintoandar/butterfree/pull/230))
* Fix broken notebook URL ([#236](https://github.com/quintoandar/butterfree/pull/236))
* Issue #77 Fix ([#245](https://github.com/quintoandar/butterfree/pull/245))

## [1.0.2](https://github.com/quintoandar/butterfree/releases/tag/1.0.2)
### Added
* [MLOP-427] Add status badges ([#219](https://github.com/quintoandar/butterfree/pull/219))

### Changed
* [MLOP-426] Change branching strategy on butterfree to use only master branch ([#216](https://github.com/quintoandar/butterfree/pull/216))

### Fixed
* [MLOP-440] Python 3.7 bump and Fixing Dependencies ([#220](https://github.com/quintoandar/butterfree/pull/220))

## [1.0.1](https://github.com/quintoandar/butterfree/releases/tag/1.0.1)
### Added
* Adding the library motto to readme and python package ([#203](https://github.com/quintoandar/butterfree/pull/203))

### Changed
* [MLOP-418] Take Butterfree's docs to the master branch ([#201](https://github.com/quintoandar/butterfree/pull/201))
* Update notebooks examples ([#205](https://github.com/quintoandar/butterfree/pull/205))
* [MLOP-417] Remove Drone yml/Drone references ([#211](https://github.com/quintoandar/butterfree/pull/211))

### Fixed
* Fixing an attribute reference error and raising a more meaningful exception when an anonymous function is passed to an AggregatedTransform ([#206](https://github.com/quintoandar/butterfree/pull/206))
* Fix schema and reports. ([#207](https://github.com/quintoandar/butterfree/pull/207))
* Fixes type hints for `clients` module ([#198](https://github.com/quintoandar/butterfree/pull/198))

## [1.0.0](https://github.com/quintoandar/butterfree/releases/tag/1.0.0)
### Added
* [MLOP-415] GitHub Actions CI ([#199](https://github.com/quintoandar/butterfree/pull/199))
* [MLOP-390] Create method to get feature sets' description ([#188](https://github.com/quintoandar/butterfree/pull/188))
* [MLOP-386] Create PR Guideline on CONTRIBUTING.md ([#190](https://github.com/quintoandar/butterfree/pull/190))
* [MLOP-409] Configuration Section on Documentation ([#193](https://github.com/quintoandar/butterfree/pull/193))
* [MLOP-367] Investigate and define license for the repository ([#185](https://github.com/quintoandar/butterfree/pull/185))
* [MLOP-68] Understand how to generate, store and host documentation ([#168](https://github.com/quintoandar/butterfree/pull/168))
* [MLOP-344] Update README with documentation ([#175](https://github.com/quintoandar/butterfree/pull/175))
* [MLOP-343] Create "make update-docs" in Butterfree ([#174](https://github.com/quintoandar/butterfree/pull/174))
* [MLOP-361] Create more use cases for AggregatedFeatureSet ([#179](https://github.com/quintoandar/butterfree/pull/179))

### Changed
* Update Readme for 1.0.0 ([#197](https://github.com/quintoandar/butterfree/pull/197))
* [MLOP-404] Restructuring of Butterfree folder and imports ([#192](https://github.com/quintoandar/butterfree/pull/192))
* [MLOP-363] DB Credentials Setup Refactor ([#187](https://github.com/quintoandar/butterfree/pull/187))
* [MLOP-384] Make imports more simple ([#184](https://github.com/quintoandar/butterfree/pull/184))
* [MLOP-375] Refactor Sink and OnlineFeatureStore write to entity flow ([#182](https://github.com/quintoandar/butterfree/pull/182))
* [MLOP-365] Double check docstrings/readme/wiki for references to QuintoAndar ([#183](https://github.com/quintoandar/butterfree/pull/183))
* [MLOP-376] Change spark version on Butterfree ([#177](https://github.com/quintoandar/butterfree/pull/177))

### Removed
* [MLOP-385] Remove Butterfree Image References ([#191](https://github.com/quintoandar/butterfree/pull/191))
* Fix `__init__` ([#189](https://github.com/quintoandar/butterfree/pull/189))

### Fixed
* Fixes docs hyperlink on README ([#196](https://github.com/quintoandar/butterfree/pull/196))
* [MLOP-392] Fix docstrings for ReadtheDocs ([#186](https://github.com/quintoandar/butterfree/pull/186))

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

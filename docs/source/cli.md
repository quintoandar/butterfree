# Command-line Interface (CLI)

Butterfree has now a command-line interface, introduced with the new automatic migration ability.

As soon as you install butterfree, you can check what's available through butterfree's cli with:

```shell
$~ butterfree --help
```

### Automated Database Schema Migration

When developing your feature sets, you need also to prepare your database for the changes
to come into your Feature Store. Normally, when creating a new feature set, you needed
to manually create a new table in cassandra. Or, when creating a new feature in an existing
feature set, you needed to create new column in cassandra too.

Now, you can just use `butterfree migrate apply ...`, butterfree will scan your python
files, looking for classes that inherit from `butterfree.pipelines.FeatureSetPipeline`,
then compare its schema with the database schema where the feature set would be written.
Then it will prepare migration queries and run against the databases.

For more information, please, check `butterfree migrate apply --help` :)

### Supported databases

This functionality currently supports only the **Cassandra** database, which is the default
storage for an Online Feature Store built with Butterfree. Nonetheless, it was made with
the intent to be easily extended for other databases.

Also, each database has its own rules for schema migration commands. Some changes may
still require manual interference.
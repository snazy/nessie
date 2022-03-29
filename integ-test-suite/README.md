# Integrations Test Suite

Suite to test various release version and in-development-version combinations of Integrations
(e.g. Apache Iceberg, Trino, Deltalake) and Nessie.

Configurations (Git remotes, Git branch/tag names) of the products (Nessie, Iceberg, etc.) can be
provided implicitly via a configuration name referencing the
[settings in the configuration file](./scripts/config.in.sh) or by providing the individual settings
via shell environment variables as explained in the [configuration file](./scripts/config.in.sh).

## Test Coverage

* Runs the tests in Nessie modules that depend on Iceberg (or Deltalake, or Trino)
* Runs the tests in Iceberg that depend on Nessie
* Runs the tests in Trino that depend on Nessie
* Runs more Iceberg/Trino related tests using various Spark versions in the integrations tests
  in [this independent maven project](./tests).

The tests in [the independent maven project](./tests) are quite rudimentary at the moment.
A bunch work "scenario based" tests need to be added for:

* Iceberg w/ Spark 3.1 w/ Nessie Spark SQL Extensions
* Iceberg w/ Spark 3.2 w/ Nessie Spark SQL Extensions
* Iceberg w/ Flink
* Trino w/ Iceberg
* Deltalake w/ Spark 3.2 w/ Nessie Spark SQL Extensions

## Nessie Integrations and patches

Nessie and the projects/products integrating Nessie change and likely require changes in their
code bases to work with each other. To allow testing arbitrary combinations of Nessie version,
Iceberg version, Trino version, etc. each project/product is references using a base Git reference
(provided using a Git remote URL plus branch/tag name) _plus_ an optional patch Git reference
(also consists of a Git remote URL plus branch/tag name).

The following example configuration uses the HEAD of `projectnessie/nessie/main` plus a patch
in the branch `integ-bump/iceberg` in the `snazy/nessie` fork of `projectnessie/nessie`.
```
tools_HEAD_nessie=https://github.com/projectnessie/nessie.git@main+https://github.com/snazy/nessie@integ-bump/iceberg
tools_HEAD_iceberg=https://github.com/apache/iceberg@master
tools_HEAD_trino=https://github.com/snazy/trino@add-nessie-support+https://github.com/snazy/trino.git@iceberg-0.14
```

"Patches" are applied using the "Git diff" of the HEAD of the branch `integ-bump/iceberg` in
`snazy/nessie` up to the common ancestor of the patch branch and the base branch `main` in
`projectnessie/nessie`.

Note: When you run the Integrations Test Suite locally, your local branch pointing to the configured
Git repo + tag/branch name will be used, so you can test changes locally. For example, if you have
a branch `main` tracking the upstream branch `main` from `projectnessie/nessie` in your local Git
repo, the local `main` branch will be used.

## Running the Integrations Test Suite

### Nessie CI integration

Pull requests with the `pr-integrations` label will run the 
[`PR Integrations Tests` GitHub workflow](../.github/workflows/pull-request-integ.yml) using
the configuration named `HEAD` referenced in the [configuration file](./scripts/config.in.sh).

### Local runs

Run the [script `run-locally.sh`](./run-locally.sh), which defaults to the configuration
named `HEAD` referenced in the [configuration file](./scripts/config.in.sh)

## Developing Tests in the Integrations Test Suite

Open the Maven project in the [`tests/`](./tests) directory as a separate project in your IDE.

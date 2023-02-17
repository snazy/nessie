# Project Nessie

[![Website](https://img.shields.io/badge/Website-projectnessie.org-blue.svg)](https://projectnessie.org/)
[![Group Discussion](https://img.shields.io/badge/Discussion-groups.google.com-blue.svg)](https://groups.google.com/g/projectnessie)
[![Twitter](https://img.shields.io/twitter/url/http/shields.io.svg?style=social&label=Follow)](https://twitter.com/projectnessie)

[![Maven Central](https://img.shields.io/maven-central/v/org.projectnessie/nessie)](https://search.maven.org/artifact/org.projectnessie/nessie)
[![PyPI](https://img.shields.io/pypi/v/pynessie.svg)](https://pypi.python.org/pypi/pynessie)
[![Docker](https://img.shields.io/docker/v/projectnessie/nessie/latest?label=Docker)](https://hub.docker.com/r/projectnessie/nessie)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/nessie)](https://artifacthub.io/packages/search?repo=nessie)

[![Build Status](https://github.com/projectnessie/nessie/workflows/Main%20CI/badge.svg)](https://github.com/projectnessie/nessie/actions/workflows/main.yml)
[![Query Engine Integrations](https://github.com/projectnessie/query-engine-integration-tests/actions/workflows/main.yml/badge.svg)](https://github.com/projectnessie/query-engine-integration-tests/actions/workflows/main.yml)
[![Java 17+18](https://github.com/projectnessie/nessie/actions/workflows/newer-java.yml/badge.svg)](https://github.com/projectnessie/nessie/actions/workflows/newer-java.yml)
[![Windows Build Check](https://github.com/projectnessie/nessie/actions/workflows/ci-win.yml/badge.svg)](https://github.com/projectnessie/nessie/actions/workflows/ci-win.yml)
[![macOS Build Check](https://github.com/projectnessie/nessie/actions/workflows/ci-mac.yml/badge.svg)](https://github.com/projectnessie/nessie/actions/workflows/ci-mac.yml)
[![codecov](https://codecov.io/gh/projectnessie/nessie/branch/main/graph/badge.svg?token=W9J9ZUYO1Y)](https://codecov.io/gh/projectnessie/nessie)

Project Nessie is a Transactional Catalog for Data Lakes with Git-like semantics.

More information can be found at [projectnessie.org](https://projectnessie.org/).

Nessie supports Iceberg Tables/Views and Delta Lake Tables. Additionally, Nessie is focused on working with the widest range of tools possible, which can be seen in the [feature matrix](https://projectnessie.org/tools/#feature-matrix).

## Using Nessie

You can quickly get started with Nessie by using our small, fast docker image.

```
docker pull projectnessie/nessie
docker run -p 19120:19120 projectnessie/nessie
```
_For trying Nessie image with different configuration options, refer to the templates under the [docker module](./docker#readme)._<br>

A local [Web UI](https://projectnessie.org/tools/ui/) will be available at this point.

Then install the Nessie CLI tool (to learn more about CLI tool and how to use it, check [Nessie CLI Documentation](https://projectnessie.org/tools/cli/)).

```
pip install pynessie
```

From there, you can use one of our technology integrations such those for 

* [Spark via Iceberg](https://projectnessie.org/tools/iceberg/spark/)
* [Hive via Iceberg](https://projectnessie.org/tools/iceberg/hive/)
* [Spark via Delta Lake](https://projectnessie.org/tools/deltalake/spark/)

To learn more about all supported integrations and tools, check [here](https://projectnessie.org/tools/) 

Have fun! We have a Google Group and a Slack channel we use for both developers and 
users. Check them out [here](https://projectnessie.org/community/).

### Authentication

By default, Nessie servers run with authentication disabled and all requests are processed under the "anonymous"
user identity.

Nessie supports bearer tokens and uses [OpenID Connect](https://openid.net/connect/) for validating them.

Authentication can be enabled by setting the following Quarkus properties:
* `nessie.server.authentication.enabled=true`
* `quarkus.oidc.auth-server-url=<OpenID Server URL>`
* `quarkus.oidc.client-id=<Client ID>`

#### Experimenting with Nessie Authentication in Docker

One can start the `projectnessie/nessie` docker image in authenticated mode by setting
the properties mentioned above via docker environment variables. For example:

```
docker run -p 19120:19120 -e QUARKUS_OIDC_CLIENT_ID=<Client ID> -e QUARKUS_OIDC_AUTH_SERVER_URL=<OpenID Server URL> -e NESSIE_SERVER_AUTHENTICATION_ENABLED=true --network host projectnessie/nessie
```

## Building and Developing Nessie

### Requirements

- JDK 11 or higher: JDK11 or higher is needed to build Nessie (artifacts are built 
  for Java 8)

### Installation

Clone this repository:
```bash
git clone https://github.com/projectnessie/nessie
cd nessie
```

Then open the project in IntelliJ or Eclipse, or just use the IDEs to clone this github repository.

Refer to [CONTRIBUTING](./CONTRIBUTING.md) for build instructions.

### Compatibility

Nessie Iceberg's integration is compatible with Iceberg as in the following table:

| Nessie version | Iceberg version | Spark version                                                        | Hive version | Flink version  | Presto version          |
|----------------|-----------------|----------------------------------------------------------------------|--------------|----------------|-------------------------|
| 0.49.0         | 1.1.0           | 3.1.x (Scala 2.12), 3.2.x (Scala 2.12+2.13), 3.3.x (Scala 2.12+2.13) | n/a          | 1.14.x, 1.15.x | 0.276.x, 0.277, 0.278.x |

Nessie Delta Lake's integration is compatible with Delta Lake as in the following table:

| Nessie version | Delta Lake version              | Spark version | 
|----------------|---------------------------------|---------------|
| 0.49.0         | [Custom](#delta-lake-artifacts) | 3.2.X         |

#### Delta Lake artifacts

Nessie required some minor changes to Delta for full support of branching and history. These changes are currently being integrated into the [mainline repo](https://github.com/delta-io/delta). Until these have been merged we have provided custom builds in [our fork](https://github.com/projectnessie/delta) which can be downloaded from a separate maven repository. 

### Distribution
To run:
1. configuration in `servers/quarkus-server/src/main/resources/application.properties`
2. execute `./gradlew quarkusDev`
3. go to `http://localhost:19120`

### UI 
To run the ui (from `ui` directory):
1. If you are running in test ensure that `setupProxy.js` points to the correct api instance. This ensures we avoid CORS
issues in testing
2. `npm install` will install dependencies
3. `npm run start` to start the ui in development mode via node

To deploy the ui (from `ui` directory):
1. `npm install` will install dependencies
2. `npm build` will minify and collect the package for deployment in `build`
3. the `build` directory can be deployed to any static hosting environment or run locally as `serve -s build`

### Docker image

When running `./gradlew :nessie-quarkus:quarkusBuild -Pdocker -Pnative` a docker image will
be created at `projectnessie/nessie` which can be started with `docker run -p 19120:19120 projectnessie/nessie`
and the relevant environment variables. Environment variables  are specified as per
https://github.com/eclipse/microprofile-config/blob/master/spec/src/main/asciidoc/configsources.asciidoc#default-configsources  


### AWS Lambda
You can also deploy to AWS lambda function by following the steps in `servers/lambda/README.md`

## Nessie related repositories

* [Nessie Demos](https://github.com/projectnessie/nessie-demos): Demos for Nessie
* [CEL Java](https://github.com/projectnessie/cel-java): Java port of the Common Expression Language
* [Nessie apprunner](https://github.com/projectnessie/nessie-apprunner): Maven and Gradle plugins to use Nessie in integration tests.

## Contributing

### Code Style

The Nessie project uses the Google Java Code Style, scalafmt and pep8.
See [CONTRIBUTING.md](./CONTRIBUTING.md) for more information.



HELLO


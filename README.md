# Project Nessie

Project Nessie is a Transactional Catalog for Data Lakes with Git-like semantics.

[![Zulip](https://img.shields.io/badge/Zulip-Chat-blue?color=3d4db3&logo=zulip&style=for-the-badge&logoColor=white)](https://project-nessie.zulipchat.com/)
[![Group Discussion](https://img.shields.io/badge/Discussion-Groups-blue.svg?color=3d4db3&logo=google&style=for-the-badge&logoColor=white)](https://groups.google.com/g/projectnessie)
[![Twitter](https://img.shields.io/badge/Twitter-Follow_Us-blue?color=3d4db3&logo=twitter&style=for-the-badge&logoColor=white)](https://twitter.com/projectnessie)
[![Website](https://img.shields.io/badge/https-projectnessie.org-blue?color=3d4db3&logo=firefox&style=for-the-badge&logoColor=white)](https://projectnessie.org/)

[![Maven Central](https://img.shields.io/maven-central/v/org.projectnessie/nessie?label=Maven%20Central&logo=apachemaven&color=3f6ec6&style=for-the-badge&logoColor=white)](https://search.maven.org/artifact/org.projectnessie/nessie)
[![PyPI](https://img.shields.io/pypi/v/pynessie.svg?label=PyPI&logo=python&color=3f6ec6&style=for-the-badge&logoColor=white)](https://pypi.python.org/pypi/pynessie)
[![quay.io Docker](https://img.shields.io/maven-central/v/org.projectnessie/nessie?label=quay.io+Docker&logo=docker&color=3f6ec6&style=for-the-badge&logoColor=white)](https://quay.io/repository/projectnessie/nessie?tab=tags)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/nessie&color=3f6ec6&labelColor=&style=for-the-badge&logoColor=white)](https://artifacthub.io/packages/search?repo=nessie)
[![Swagger Hub](https://img.shields.io/badge/swagger%20hub-nessie-3f6ec6?style=for-the-badge&logo=swagger&link=https%3A%2F%2Fapp.swaggerhub.com%2Fapis%2Fprojectnessie%2Fnessie)](https://app.swaggerhub.com/apis/projectnessie/nessie)


[![Build Status](https://img.shields.io/github/actions/workflow/status/projectnessie/nessie/main.yml?label=Main%20CI&logo=Github&style=flat-square)](https://github.com/projectnessie/nessie/actions/workflows/main.yml)
[![Query Engines CI](https://img.shields.io/github/actions/workflow/status/projectnessie/query-engine-integration-tests/main.yml?label=Nessie%2FIceberg%20in-dev&logo=Github&style=flat-square)](https://github.com/projectnessie/query-engine-integration-tests/actions/workflows/main.yml)
[![Java 17+18](https://img.shields.io/github/actions/workflow/status/projectnessie/nessie/newer-java.yml?label=Java%2017%2B&logo=Github&style=flat-square)](https://github.com/projectnessie/nessie/actions/workflows/newer-java.yml)
[![Windows Build](https://img.shields.io/github/actions/workflow/status/projectnessie/nessie/ci-win.yml?label=Windows&logo=windows&style=flat-square)](https://github.com/projectnessie/nessie/actions/workflows/ci-win.yml)
[![macOS Build](https://img.shields.io/github/actions/workflow/status/projectnessie/nessie/ci-mac.yml?label=macOS&logo=apple&style=flat-square)](https://github.com/projectnessie/nessie/actions/workflows/ci-mac.yml)

More information can be found at [projectnessie.org](https://projectnessie.org/).

Nessie supports Iceberg Tables/Views and Delta Lake Tables. Additionally, Nessie is focused on working with the widest range of tools possible, which can be seen in the [feature matrix](https://projectnessie.org/tools/#feature-matrix).

## Using Nessie

You can quickly get started with Nessie by using our small, fast docker image.

**IMPORTANT NOTE** Nessie moves away from `docker.io` to GitHub's container registry `ghcr.io`,
and also `quay.io`. Previous releases are already available on both ghcr.io and quay.io. Please
update references to `projectnessie/nessie` in your code to either `ghcr.io/projectnessie/nessie`
or `quay.io/projectnessie/nessie`.

```
docker pull ghcr.io/projectnessie/nessie
docker run -p 19120:19120 ghcr.io/projectnessie/nessie
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

```shell
docker run -p 19120:19120 \
  -e QUARKUS_OIDC_CLIENT_ID=<Client ID> \
  -e QUARKUS_OIDC_AUTH_SERVER_URL=<OpenID Server URL> \
  -e NESSIE_SERVER_AUTHENTICATION_ENABLED=true \
  --network host \
  ghcr.io/projectnessie/nessie
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

| Nessie version | Iceberg version | Spark version                                                        | Hive version | Flink version          | Presto version        |
|----------------|-----------------|----------------------------------------------------------------------|--------------|------------------------|-----------------------|
| 0.54.0         | 1.2.0           | 3.1.x (Scala 2.12), 3.2.x (Scala 2.12+2.13), 3.3.x (Scala 2.12+2.13) | n/a          | 1.14.x, 1.15.x, 1.16.x | 0.277, 0.278.x, 0.279 |

Nessie Delta Lake's integration is compatible with Delta Lake as in the following table:

| Nessie version | Delta Lake version              | Spark version | 
|----------------|---------------------------------|---------------|
| 0.54.0         | [Custom](#delta-lake-artifacts) | 3.2.X         |

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

Official Nessie images are built with support for [multiplatform builds](./tools/dockerbuild#readme). But to quickly
build a docker image for testing purposes, simply run the following command:

```shell
./gradlew :nessie-quarkus:clean :nessie-quarkus:quarkusBuild
docker build -f ./tools/dockerbuild/docker/Dockerfile-jvm -t nessie-unstable:latest ./servers/quarkus-server 
```

If you prefer to build a native docker image instead, add `-Pnative` and use a dedicated Dockerfile:

```shell
./gradlew :nessie-quarkus:clean :nessie-quarkus:quarkusBuild -Pnative
docker build -f ./tools/dockerbuild/docker/Dockerfile-native -t nessie-unstable:latest ./servers/quarkus-server 
```

Warning: building native images this way may not work properly on macOS with Apple Silicon processors.

Check that your image is available locally:

```shell
docker images
```

You should see something like this:

```
REPOSITORY       TAG     IMAGE ID       CREATED          SIZE
nessie-unstable  latest  24bb4c7bd696   15 seconds ago   555MB
```

Once this is done you can run your image with `docker run -p 19120:19120 quay.io/nessie-unstable:latest`, passing the relevant
environment variables, if any. Environment variables names must follow MicroProfile Config's [mapping
rules](https://github.com/eclipse/microprofile-config/blob/master/spec/src/main/asciidoc/configsources.asciidoc#environment-variables-mapping-rules).

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

## Acknowledgements

See [ACKNOWLEDGEMENTS.md](ACKNOWLEDGEMENTS.md)

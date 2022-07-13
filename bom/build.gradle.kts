/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
  `java-platform`
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Bill of Materials (BOM)"

dependencies {
  constraints {
    api(rootProject)
    api(project(":nessie-clients"))
    api(project(":nessie-client"))
    api(project(":nessie-spark-antlr-runtime"))
    api(project(":nessie-compatibility"))
    api(project(":nessie-compatibility-common"))
    api(project(":nessie-compatibility-tests"))
    api(project(":nessie-compatibility-jersey"))
    api(project(":nessie-gc"))
    api(project(":nessie-model"))
    api(project(":nessie-perftest"))
    api(project(":nessie-server-parent"))
    api(project(":nessie-jaxrs"))
    api(project(":nessie-jaxrs-testextension"))
    api(project(":nessie-jaxrs-tests"))
    api(project(":nessie-quarkus-common"))
    api(project(":nessie-quarkus-cli"))
    api(project(":nessie-quarkus"))
    api(project(":nessie-quarkus-tests"))
    api(project(":nessie-rest-services"))
    api(project(":nessie-services"))
    api(project(":nessie-server-store"))
    api(project(":nessie-server-store-proto"))
    api(project(":nessie-tools"))
    api(project(":nessie-content-generator"))
    api(project(":nessie-ui"))
    api(project(":nessie-versioned"))
    api(project(":nessie-versioned-persist"))
    api(project(":nessie-versioned-persist-adapter"))
    api(project(":nessie-versioned-persist-bench"))
    api(project(":nessie-versioned-persist-dynamodb"))
    api(project(":nessie-versioned-persist-dynamodb")) { testJarCapability() }
    api(project(":nessie-versioned-persist-in-memory"))
    api(project(":nessie-versioned-persist-in-memory")) { testJarCapability() }
    api(project(":nessie-versioned-persist-mongodb"))
    api(project(":nessie-versioned-persist-mongodb")) { testJarCapability() }
    api(project(":nessie-versioned-persist-non-transactional"))
    api(project(":nessie-versioned-persist-rocks"))
    api(project(":nessie-versioned-persist-rocks")) { testJarCapability() }
    api(project(":nessie-versioned-persist-serialize"))
    api(project(":nessie-versioned-persist-serialize-proto"))
    api(project(":nessie-versioned-persist-store"))
    api(project(":nessie-versioned-persist-tests"))
    api(project(":nessie-versioned-persist-transactional"))
    api(project(":nessie-versioned-persist-transactional")) { testJarCapability() }
    api(project(":nessie-versioned-spi"))
    api(project(":nessie-versioned-tests"))
    if (!isIntegrationsTestingEnabled()) {
      api(project(":nessie-deltalake"))
      api(project(":iceberg-views"))
      api(project(":nessie-spark-extensions"))
      api(project(":nessie-spark-3.2-extensions"))
      api(project(":nessie-spark-extensions-grammar"))
      api(project(":nessie-spark-extensions-base"))
      api(project(":nessie-gc-base"))
    }
  }
}

javaPlatform { allowDependencies() }

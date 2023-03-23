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

import org.apache.tools.ant.taskdefs.condition.Os

plugins {
  `java-library`
  jacoco
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Import/Export"

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-transfer-proto"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-store"))
  implementation(project(":nessie-versioned-storage-transitional"))

  implementation(platform(libs.jackson.bom))
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)

  compileOnly(libs.microprofile.openapi)
  compileOnly(libs.errorprone.annotations)
  implementation(libs.guava)

  // javax/jakarta
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  testFixturesImplementation(platform(libs.jackson.bom))
  testFixturesImplementation(libs.jackson.databind)
  testFixturesImplementation(libs.guava)
  testFixturesApi(libs.jakarta.annotation.api)
  testFixturesApi(project(":nessie-client"))
  testFixturesApi(project(":nessie-server-store"))
  testFixturesApi(project(":nessie-versioned-transfer-proto"))
  testFixturesApi(project(":nessie-versioned-spi"))
  testFixturesApi(project(":nessie-versioned-persist-adapter"))
  testFixturesApi(project(":nessie-versioned-persist-testextension"))
  testFixturesApi(project(":nessie-versioned-persist-in-memory"))
  testFixturesApi(project(":nessie-versioned-persist-in-memory-test"))
  intTestImplementation(project(":nessie-versioned-persist-mongodb"))
  intTestImplementation(project(":nessie-versioned-persist-mongodb-test"))
  intTestImplementation(project(":nessie-versioned-persist-dynamodb"))
  intTestImplementation(project(":nessie-versioned-persist-dynamodb-test"))
  intTestImplementation(project(":nessie-versioned-persist-rocks"))
  intTestImplementation(project(":nessie-versioned-persist-rocks-test"))
  intTestImplementation(project(":nessie-versioned-persist-transactional"))
  intTestImplementation(project(":nessie-versioned-persist-transactional-test"))

  testFixturesApi(project(":nessie-versioned-storage-cache"))
  testFixturesApi(project(":nessie-versioned-storage-common"))
  testFixturesApi(project(":nessie-versioned-storage-inmemory"))
  testFixturesApi(project(":nessie-versioned-storage-testextension"))
  intTestImplementation(project(":nessie-versioned-storage-cassandra"))
  intTestImplementation(project(":nessie-versioned-storage-dynamodb"))
  intTestImplementation(project(":nessie-versioned-storage-jdbc"))
  intTestImplementation(project(":nessie-versioned-storage-mongodb"))
  intTestImplementation(project(":nessie-versioned-storage-rocksdb"))

  intTestRuntimeOnly(libs.h2)

  // javax/jakarta
  testFixturesImplementation(libs.jakarta.annotation.api)

  testFixturesImplementation(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
}

// Issue w/ testcontainers/podman in GH workflows :(
if (Os.isFamily(Os.FAMILY_MAC) && System.getenv("CI") != null) {
  tasks.named<Test>("intTest") { this.enabled = false }
}

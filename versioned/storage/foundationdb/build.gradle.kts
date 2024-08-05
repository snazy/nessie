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
  id("nessie-conventions-server")
  id("nessie-jacoco")
}

publishingHelper { mavenName = "Nessie - Storage - FoundationDB" }

description = "Storage implementation for FoundationDB."

dependencies {
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-common-proto"))
  implementation(project(":nessie-versioned-storage-common-serialize"))
  implementation(libs.foundationdb.java)

  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)

  compileOnly(libs.errorprone.annotations)
  implementation(libs.agrona)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  intTestImplementation(project(":nessie-versioned-storage-foundationdb-tests"))
  intTestImplementation(project(":nessie-versioned-storage-common-tests"))
  intTestImplementation(project(":nessie-versioned-storage-testextension"))
  intTestImplementation(project(":nessie-versioned-tests"))
  intTestImplementation(platform(libs.junit.bom))
  intTestImplementation(libs.bundles.junit.testing)
  intTestRuntimeOnly(libs.logback.classic)
  intTestRuntimeOnly(platform(libs.testcontainers.bom))
  intTestRuntimeOnly("org.testcontainers:testcontainers")
}

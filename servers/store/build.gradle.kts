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
  `java-library`
  jacoco
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Server - Store"

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-spi"))
  api(project(":nessie-server-store-proto"))
  implementation(libs.guava)

  implementation(platform(libs.jackson.bom))
  implementation(libs.jackson.databind)
  compileOnly(libs.jackson.annotations)

  compileOnly(libs.microprofile.openapi)
  compileOnly(libs.jakarta.validation.api)

  testImplementation(libs.guava)
  testCompileOnly(libs.microprofile.openapi)

  testCompileOnly(platform(libs.jackson.bom))
  testCompileOnly(libs.jackson.annotations)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

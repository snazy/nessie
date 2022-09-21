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

extra["maven.name"] = "Nessie - Versioned - Persist - Transactional/test-support"

dependencies {
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-transactional"))
  implementation(project(":nessie-versioned-persist-testextension"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-tests"))

  implementation(libs.guava)
  implementation(libs.findbugs.jsr305)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)
  implementation(libs.slf4j.api)

  implementation(libs.agroal.pool)
  compileOnly(libs.h2)
  compileOnly(libs.postgresql)

  implementation(libs.testcontainers.postgresql)
  implementation(libs.testcontainers.cockroachdb)
  implementation(libs.docker.java.api)
}

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
  id("org.projectnessie.nessie-project")
  id("org.projectnessie.buildsupport.attach-test-jar")
}

extra["maven.artifactId"] = "nessie-versioned-persist-transactional"

extra["maven.name"] = "Nessie - Versioned - Persist - Transactional"

dependencies {
  implementation(platform(rootProject))
  annotationProcessor(platform(rootProject))

  implementation(projects.versioned.persist.adapter)
  implementation(projects.versioned.persist.serialize)
  implementation(projects.versioned.spi)
  implementation("com.google.guava:guava")
  implementation("com.google.code.findbugs:jsr305")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")
  implementation("org.slf4j:slf4j-api")

  compileOnly("io.agroal:agroal-pool")
  compileOnly("com.h2database:h2")
  compileOnly("org.postgresql:postgresql")

  testImplementation(platform(rootProject))
  testAnnotationProcessor(platform(rootProject))
  testImplementation(projects.versioned.tests)
  testCompileOnly("org.immutables:value-annotations")
  testAnnotationProcessor("org.immutables:value-processor")
  testImplementation(projects.versioned.persist.persistTests)
  testImplementation("org.testcontainers:testcontainers")
  testImplementation("org.testcontainers:postgresql")
  testImplementation("org.testcontainers:cockroachdb")
  testImplementation("com.github.docker-java:docker-java-api")
  testImplementation("io.agroal:agroal-pool")
  testRuntimeOnly("com.h2database:h2")
  testRuntimeOnly("org.postgresql:postgresql")

  testImplementation("org.assertj:assertj-core")
  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.named<Test>("test") { maxParallelForks = Runtime.getRuntime().availableProcessors() }

tasks.named<Test>("intTest") {
  systemProperty("it.nessie.dbs", System.getProperty("it.nessie.dbs", "postgres"))
}

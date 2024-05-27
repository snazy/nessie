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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.apache.tools.ant.taskdefs.condition.Os

plugins {
  id("nessie-conventions-server")
  id("nessie-jacoco")
  id("nessie-shadow-jar")
  id("nessie-license-report")
}

extra["maven.name"] = "Nessie - CLI"

configurations.all { exclude(group = "org.projectnessie.nessie", module = "nessie-model") }

val versionIceberg = libs.versions.iceberg.get()

val nessieQuarkusServer by configurations.creating

dependencies {
  implementation(project(":nessie-model-quarkus"))
  implementation(project(":nessie-client"))
  implementation(project(":nessie-cli-grammar"))
  implementation(project(":nessie-notice"))

  implementation(libs.jline)
  implementation(libs.picocli)

  implementation(platform("org.apache.iceberg:iceberg-bom:$versionIceberg"))
  implementation("org.apache.iceberg:iceberg-core")
  runtimeOnly(libs.hadoop.common) { isTransitive = false }

  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.microprofile.openapi)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  runtimeOnly(libs.logback.classic)

  testFixturesApi(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
  testFixturesImplementation(project(":nessie-client"))
  testFixturesImplementation(project(":nessie-cli-grammar"))
  testFixturesImplementation(libs.jline)

  testImplementation(project(":nessie-jaxrs-testextension"))

  testImplementation(project(":nessie-versioned-storage-inmemory-tests"))

  testCompileOnly(libs.immutables.value.annotations)

  intTestImplementation(project(":nessie-object-storage-mock"))
  intTestImplementation(project(":nessie-catalog-format-iceberg"))
  intTestImplementation(project(":nessie-catalog-format-iceberg-fixturegen"))
  intTestImplementation(project(":nessie-catalog-files-api"))
  intTestImplementation(project(":nessie-catalog-files-impl"))
  intTestImplementation(libs.nessie.runner.common)
  intTestImplementation(platform(libs.awssdk.bom))
  intTestImplementation("software.amazon.awssdk:s3")
  intTestImplementation("software.amazon.awssdk:apache-client") {
    exclude("commons-logging", "commons-logging")
  }

  nessieQuarkusServer(project(":nessie-quarkus", "quarkusRunner"))
}

tasks.withType<ProcessResources>().configureEach {
  from("src/main/resources") { duplicatesStrategy = DuplicatesStrategy.INCLUDE }
}

tasks.named<ShadowJar>("shadowJar").configure {
  manifest { attributes["Main-Class"] = "org.projectnessie.nessie.cli.cli.NessieCliMain" }
}

// Testcontainers is not supported on Windows :(
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.named<Test>("intTest").configure { this.enabled = false }
}

// Issue w/ testcontainers/podman in GH workflows :(
if (Os.isFamily(Os.FAMILY_MAC) && System.getenv("CI") != null) {
  tasks.named<Test>("intTest").configure { this.enabled = false }
}

tasks.named<Test>("intTest").configure {
  // Spark keeps a lot of stuff around in the JVM, breaking tests against different Iceberg
  // catalogs, so give every test class its own JVM
  forkEvery = 1
  inputs.files(nessieQuarkusServer)
  val execJarProvider =
    configurations.named("nessieQuarkusServer").map { c ->
      val file = c.fileCollection(*c.dependencies.toTypedArray()).files.first()
      listOf("-Dnessie.exec-jar=${file.absolutePath}")
    }
  jvmArgumentProviders.add(CommandLineArgumentProvider { execJarProvider.get() })
}

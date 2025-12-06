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
import com.github.jengelman.gradle.plugins.shadow.transformers.DeduplicatingResourceTransformer
import com.github.jengelman.gradle.plugins.shadow.transformers.PropertiesFileTransformer
import kotlin.jvm.java

plugins {
  alias(libs.plugins.nessie.run)
  id("nessie-conventions-java11")
  id("nessie-shadow-jar")
  id("nessie-license-report")
}

dependencies {
  implementation(project(":nessie-client"))
  runtimeOnly(libs.httpclient5)

  implementation(libs.picocli)
  // TODO help picocli to make their annotation-processor incremental
  annotationProcessor(libs.picocli.codegen)
  implementation(libs.guava)

  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.microprofile.openapi)
  runtimeOnly(libs.slf4j.api)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  testFixturesImplementation(project(":nessie-client"))

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesApi(libs.microprofile.openapi)
  testFixturesCompileOnly(libs.picocli)
  testFixturesCompileOnly(platform(libs.jackson.bom))
  testFixturesCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testFixturesApi(libs.httpclient5)

  testImplementation(project(":nessie-jaxrs-testextension"))

  testImplementation(project(":nessie-versioned-storage-inmemory-tests"))

  nessieQuarkusServer(project(":nessie-quarkus", "quarkusRunner"))
}

nessieQuarkusApp {
  includeTask(tasks.named<Test>("intTest"))
  environmentNonInput.put("HTTP_ACCESS_LOG_LEVEL", testLogLevel())
  jvmArgumentsNonInput.add("-XX:SelfDestructTimer=30")
  systemProperties.put("nessie.server.send-stacktrace-to-client", "true")
}

tasks.named<ShadowJar>("shadowJar").configure {
  manifest {
    attributes["Main-Class"] = "org.projectnessie.tools.contentgenerator.cli.NessieContentGenerator"
  }

  // These 2 transformers effectively prevent having unexpected duplicates in the shadow jar.
  // But retain duplicate entries from _known_ different dependency _versions_ (shaded and unshaded
  // ones).
  transform(PropertiesFileTransformer::class.java) {
    // Check all pom.properties (catches duplicate dependencies)
    include("META-INF/maven/*/*/pom.properties")

    // Netty has this in every jar
    include("META-INF/io.netty.versions.properties")
    // Ignore property duplicates for Netty, grpc brings a shaded Netty as well
    //ignoreDuplicates.include("META-INF/io.netty.versions.properties")
  }
  transform(DeduplicatingResourceTransformer::class.java)
}

tasks.named<Test>("intTest").configure { systemProperty("expectedNessieVersion", project.version) }

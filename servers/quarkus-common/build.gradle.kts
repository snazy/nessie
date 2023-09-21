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
  id("nessie-conventions-quarkus")
  id("nessie-jacoco")
}

extra["maven.name"] = "Nessie - Quarkus Common"

configurations.configureEach {
  // Avoids dependency resolution error since Quarkus 3.3:
  // Cannot select module with conflict on capability 'com.google.guava:listenablefuture:1.0' also
  //   provided by
  //   [com.google.guava:listenablefuture:9999.0-empty-to-avoid-conflict-with-guava(runtime)]
  resolutionStrategy.capabilitiesResolution.withCapability("com.google.guava:listenablefuture") {
    select("com.google.guava:guava:0")
  }
}

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-server-store"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-store"))
  implementation(project(":nessie-versioned-persist-in-memory"))
  implementation(project(":nessie-versioned-persist-non-transactional"))
  implementation(project(":nessie-versioned-persist-rocks"))
  implementation(project(":nessie-versioned-persist-dynamodb"))
  implementation(project(":nessie-versioned-persist-mongodb"))
  implementation(project(":nessie-versioned-persist-transactional"))

  implementation(project(":nessie-versioned-storage-bigtable"))
  implementation(project(":nessie-versioned-storage-cache"))
  implementation(project(":nessie-versioned-storage-cassandra"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-dynamodb"))
  implementation(project(":nessie-versioned-storage-inmemory"))
  implementation(project(":nessie-versioned-storage-jdbc"))
  implementation(project(":nessie-versioned-storage-mongodb"))
  implementation(project(":nessie-versioned-storage-rocksdb"))
  implementation(project(":nessie-versioned-storage-store"))

  implementation(enforcedPlatform(libs.quarkus.bom)) {
    dependencies {
      implementation("io.opentelemetry:opentelemetry-opencensus-shim:1.29.0-alpha") // for Google BigTable
    }
  }
  implementation("io.quarkus:quarkus-mongodb-client")
  implementation("io.quarkus:quarkus-hibernate-validator")
  implementation("io.quarkus:quarkus-agroal")
  implementation("io.quarkus:quarkus-jdbc-postgresql")
  implementation("io.quarkus:quarkus-opentelemetry")
  implementation("io.quarkus:quarkus-micrometer")
  implementation(enforcedPlatform(libs.quarkus.amazon.services.bom))
  implementation("io.quarkiverse.amazonservices:quarkus-amazon-dynamodb")
  implementation("software.amazon.awssdk:apache-client") {
    exclude("commons-logging", "commons-logging")
  }
  implementation(enforcedPlatform(libs.quarkus.google.cloud.services.bom))
  implementation("io.quarkiverse.googlecloudservices:quarkus-google-cloud-bigtable")
  implementation(enforcedPlatform(libs.quarkus.cassandra.bom))
  implementation("com.datastax.oss.quarkus:cassandra-quarkus-client")

  implementation("org.jboss.slf4j:slf4j-jboss-logmanager")
  implementation("io.opentelemetry:opentelemetry-opencensus-shim") // for Google BigTable
  implementation("io.micrometer:micrometer-core")

  implementation(libs.guava)

  // javax/jakarta
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(libs.microprofile.openapi)
}

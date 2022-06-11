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

if (!JavaVersion.current().isCompatibleWith(JavaVersion.VERSION_11)) {
  throw GradleException("Build requires Java 11")
}

val baseVersion = file("version.txt").readText().trim()

pluginManagement {
  // Cannot use a settings-script global variable/value, so pass the 'versions' Properties via
  // settings.extra around.
  val versions = java.util.Properties()
  settings.extra["nessieBuild.versions"] = versions

  repositories {
    mavenCentral() // prefer Maven Central, in case Gradle's repo has issues
    gradlePluginPortal()
    if (System.getProperty("withMavenLocal").toBoolean()) {
      mavenLocal()
    }
  }

  plugins {
    id("com.diffplug.spotless") version "6.7.0"
    id("com.github.johnrengelman.plugin-shadow") version "7.1.2"
    id("com.github.node-gradle.node") version "3.3.0"
    id("com.github.vlsi.jandex") version "1.80"
    id("io.gatling.gradle") version "3.7.6.3"
    id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
    id("io.quarkus") version "2.9.2.Final"
    id("me.champeau.jmh") version "0.6.6"
    id("net.ltgt.errorprone") version "2.0.2"
    id("org.jetbrains.gradle.plugin.idea-ext") version "1.1.4"
    id("org.projectnessie") version "0.27.3"
    id("org.projectnessie.buildsupport.attach-test-jar") version "0.1.5"
    id("org.projectnessie.buildsupport.checkstyle") version "0.1.5"
    id("org.projectnessie.buildsupport.errorprone") version "0.1.5"
    id("org.projectnessie.buildsupport.ide-integration") version "0.1.5"
    id("org.projectnessie.buildsupport.jacoco") version "0.1.5"
    id("org.projectnessie.buildsupport.jacoco-aggregator") version "0.1.5"
    id("org.projectnessie.buildsupport.jandex") version "0.1.5"
    id("org.projectnessie.buildsupport.protobuf") version "0.1.5"
    id("org.projectnessie.buildsupport.publishing") version "0.1.5"
    id("org.projectnessie.buildsupport.reflectionconfig") version "0.1.5"
    id("org.projectnessie.buildsupport.spotless") version "0.1.5"
    id("org.projectnessie.smallrye-open-api") version "0.1.5"

    fun pluginVersion(pluginId: String): String =
      (resolutionStrategy as org.gradle.plugin.management.internal.PluginResolutionStrategyInternal)
        .applyTo(
          org.gradle.plugin.management.internal.DefaultPluginRequest(
            pluginId,
            null,
            false,
            null,
            null
          )
        )
        .version!!

    versions["versionQuarkus"] = pluginVersion("io.quarkus")
    versions["versionErrorPronePlugin"] = pluginVersion("net.ltgt.errorprone")
    versions["versionIdeaExtPlugin"] = pluginVersion("org.jetbrains.gradle.plugin.idea-ext")
    versions["versionSpotlessPlugin"] = pluginVersion("com.diffplug.spotless")
    versions["versionJandexPlugin"] = pluginVersion("com.github.vlsi.jandex")
    versions["versionShadowPlugin"] = pluginVersion("com.github.johnrengelman.plugin-shadow")

    // The project's settings.gradle.kts is "executed" before buildSrc's settings.gradle.kts and
    // build.gradle.kts.
    //
    // Plugin and important dependency versions are defined here and shared with buildSrc via
    // a properties file, and via an 'extra' property with all other modules of the Nessie build.
    //
    // This approach works fine with GitHub's dependabot as well
    val nessieBuildVersionsFile = file("build/nessieBuild/versions.properties")
    nessieBuildVersionsFile.parentFile.mkdirs()
    nessieBuildVersionsFile.outputStream().use {
      versions.store(it, "Nessie Build versions from settings.gradle.kts - DO NOT MODIFY!")
    }
  }
}

gradle.rootProject {
  val prj = this
  val versions = settings.extra["nessieBuild.versions"] as java.util.Properties
  versions.forEach { k, v -> prj.extra[k.toString()] = v }
}

gradle.beforeProject {
  version = baseVersion
  group = "org.projectnessie"
}

include("code-coverage")

include("bom")

include("clients")

include("clients:client")

include("clients:deltalake")

include("clients:iceberg-views")

include("clients:spark-3.2-extensions")

include("clients:spark-antlr-grammar")

include("clients:spark-extensions")

include("clients:spark-extensions-base")

include("compatibility")

include("compatibility:common")

include("compatibility:compatibility-tests")

include("compatibility:jersey")

include("gc")

include("gc:gc-base")

include("model")

include("perftest")

include("perftest:gatling")

include("perftest:simulations")

include("servers")

include("servers:jax-rs")

include("servers:jax-rs-testextension")

include("servers:jax-rs-tests")

include("servers:lambda")

include("servers:quarkus-common")

include("servers:quarkus-cli")

include("servers:quarkus-server")

include("servers:quarkus-tests")

include("servers:rest-services")

include("servers:services")

include("servers:store")

include("servers:store-proto")

include("tools")

include("tools:content-generator")

include("ui")

include("versioned")

include("versioned:persist")

include("versioned:persist:adapter")

include("versioned:persist:bench")

include("versioned:persist:dynamodb")

include("versioned:persist:inmem")

include("versioned:persist:mongodb")

include("versioned:persist:nontx")

include("versioned:persist:rocks")

include("versioned:persist:serialize")

include("versioned:persist:serialize-proto")

include("versioned:persist:store")

include("versioned:persist:tests")

include("versioned:persist:tx")

include("versioned:spi")

include("versioned:tests")

if (false) {
  include("gradle:dependabot")
}

rootProject.name = "nessie"

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

// otherwise clashes with "tests" from :versioned:tests
project(":versioned:persist:tests").name = "persist-tests"

// otherwise clashes with "store" from :servers:store
project(":versioned:persist:store").name = "persist-store"

project(":clients:spark-extensions").name = "spark-31-extensions"

project(":clients:spark-3.2-extensions").name = "spark-32-extensions"

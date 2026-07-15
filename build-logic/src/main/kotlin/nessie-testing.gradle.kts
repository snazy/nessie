/*
 * Copyright (C) 2023 Dremio
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

import org.gradle.api.component.AdhocComponentWithVariants
import org.gradle.api.plugins.jvm.JvmTestSuite
import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters
import org.gradle.api.tasks.testing.Test
import org.gradle.process.CommandLineArgumentProvider

plugins {
  `java-test-fixtures`
  `jvm-test-suite`
}

gradle.sharedServices.registerIfAbsent(
  "intTestParallelismConstraint",
  TestingParallelismHelper::class.java,
) {
  val intTestParallelism =
    Integer.getInteger(
      "nessie.intTestParallelism",
      (Runtime.getRuntime().availableProcessors() / 4).coerceAtLeast(1),
    )
  maxParallelUsages = intTestParallelism
}

gradle.sharedServices.registerIfAbsent(
  "testParallelismConstraint",
  TestingParallelismHelper::class.java,
) {
  val intTestParallelism =
    Integer.getInteger(
      "nessie.testParallelism",
      (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(1),
    )
  maxParallelUsages = intTestParallelism
}

// Do not publish test fixtures via Maven. Shared, reusable test code should be published as
// a separate project to retain dependency information.
components {
  named("java", AdhocComponentWithVariants::class) {
    withVariantsFromConfiguration(configurations["testFixturesApiElements"]) { skip() }
    withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) { skip() }
  }
}

val testLogLevel = providers.systemProperty("test.log.level").getOrElse("WARN").uppercase()
val quarkusLogLevel =
  if (LogLevel.valueOf(testLogLevel).ordinal > LogLevel.INFO.ordinal) "INFO" else testLogLevel

tasks.withType<Test>().configureEach {
  val testJvmArgs = providers.gradleProperty("testJvmArgs").orNull
  jvmArgs("-XX:+HeapDumpOnOutOfMemoryError")
  if (testJvmArgs != null) {
    jvmArgs(testJvmArgs.split(" "))
  }

  systemProperty("file.encoding", "UTF-8")
  systemProperty("user.language", "en")
  systemProperty("user.country", "US")
  systemProperty("user.variant", "")

  val junitXmlOutputLocation = reports.junitXml.outputLocation

  jvmArgs(
    "-Dtest.log.level=$testLogLevel",
    "-Djunit.platform.reporting.open.xml.enabled=true",
    "-Djunit.jupiter.execution.timeout.default=5m",
  )

  jvmArgumentProviders.add(
    CommandLineArgumentProvider {
      listOf(
        "-Djunit.platform.reporting.output.dir=${junitXmlOutputLocation.get().asFile.absolutePath}"
      )
    }
  )
  environment("TESTCONTAINERS_REUSE_ENABLE", "true")

  filter { isFailOnNoMatchingTests = false }
}

if (plugins.hasPlugin("io.quarkus") || plugins.hasPlugin("io.quarkus.application")) {

  tasks.withType<Test>().configureEach {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")

    jvmArgs("--add-opens=java.base/java.util=ALL-UNNAMED")
    // Log-levels are required to be able to parse the HTTP listen URL
    jvmArgs(
      "-Dquarkus.log.level=$quarkusLogLevel",
      "-Dquarkus.log.console.level=$quarkusLogLevel",
      "-Dhttp.access.log.level=$testLogLevel",
    )

    minHeapSize = "768m"
    maxHeapSize = "4g"
  }
}

val checkTask = tasks.named("check")

testing {
  suites {
    val test =
      named<JvmTestSuite>("test") {
        useJUnitJupiter(libsRequiredVersion("junit"))

        targets.all {
          testTask.configure {
            usesService(
              gradle.sharedServices.registrations.named("testParallelismConstraint").get().service
            )
          }
        }
      }

    register<JvmTestSuite>("intTest") {
      useJUnitJupiter(libsRequiredVersion("junit"))

      dependencies { implementation.add(project()) }

      targets.all {
        testTask.configure {
          usesService(
            gradle.sharedServices.registrations.named("intTestParallelismConstraint").get().service
          )

          shouldRunAfter(test)

          systemProperty("nessie.integrationTest", "true")
        }

        checkTask.configure { dependsOn(testTask) }
      }
    }
  }
}

if (plugins.hasPlugin("io.quarkus")) {
  // This directory somehow disappears... Maybe some weird Quarkus code.
  val testFixturesDir = layout.buildDirectory.dir("resources/testFixtures")
  tasks.named("quarkusGenerateCodeTests").configure {
    doFirst { testFixturesDir.get().asFile.mkdirs() }
  }
  tasks.withType<Test>().configureEach { doFirst { testFixturesDir.get().asFile.mkdirs() } }

  val compileIntTestJavaTask = tasks.named("compileIntTestJava")
  val quarkusGenerateCodeTestsTask = tasks.named("quarkusGenerateCodeTests")
  val buildDir = layout.buildDirectory
  testing.suites.named<JvmTestSuite>("intTest") {
    targets.all {
      testTask.configure {
        compileIntTestJavaTask.configure {
          dependsOn("compileQuarkusTestGeneratedSourcesJava")
        }

        // For Quarkus...
        //
        // io.quarkus.test.junit.IntegrationTestUtil.determineBuildOutputDirectory(java.net.URL)
        // is not smart enough :(
        systemProperty("build.output.directory", buildDir.asFile.get())
        dependsOn("quarkusBuild")
      }
      checkTask.configure { dependsOn(testTask) }
    }
    sources { java.srcDirs(quarkusGenerateCodeTestsTask) }
  }
}

// Let the test's implementation config extend testImplementation, so it also inherits the
// project's "main" implementation dependencies (not just the "api" configuration)
configurations.named("intTestImplementation").configure {
  extendsFrom(configurations.getByName("testImplementation"))
}

dependencies { add("intTestImplementation", java.sourceSets.getByName("test").output.dirs) }

configurations.named("intTestRuntimeOnly").configure {
  extendsFrom(configurations.getByName("testRuntimeOnly"))
}

abstract class TestingParallelismHelper : BuildService<BuildServiceParameters.None>

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
  // antlr
  id("nessie-conventions-quarkus")
}

val congocc by configurations.creating

dependencies {
  // antlr(libs.antlr.antlr4)
  // api(libs.antlr.antlr4.runtime)
  compileOnly(libs.antlr.antlr4.runtime) // TODO remove this

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  implementation(libs.agrona)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  congocc(libs.congocc)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
  testFixturesApi("org.antlr:antlr4:${libs.antlr.antlr4.runtime.get().version}")
  testFixturesApi(libs.jakarta.annotation.api)
  testFixturesApi(libs.immutables.value.annotations)
  testFixturesApi(libs.antlr.antlr4.runtime) // TODO remove this

  testFixturesRuntimeOnly(libs.logback.classic)
}

abstract class CongoCcGenerate : JavaExec() {
  @get:InputDirectory
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val sourceDir: DirectoryProperty

  @get:OutputDirectory abstract val outputDir: DirectoryProperty
}

val genGrammarDir = project.layout.buildDirectory.dir("generated-src/congocc/main")

val generateCcc =
  tasks.register("generateCcc", CongoCcGenerate::class.java) {
    sourceDir = projectDir.resolve("src/main/congocc")
    outputDir = genGrammarDir

    classpath(congocc)

    doFirst { delete(genGrammarDir) }

    mainClass = "org.congocc.app.Main"
    workingDir(projectDir)
    argumentProviders.add(
      CommandLineArgumentProvider {
        val base =
          listOf(
            "-d",
            genGrammarDir.get().asFile.toString(),
            "-jdk17",
            "-n",
            sourceDir.get().file("nessie-cli-java.ccc").asFile.relativeTo(projectDir).toString()
          )
        if (logger.isInfoEnabled) base else (base + listOf("-q"))
      }
    )
  }

tasks.named("compileJava") { dependsOn(generateCcc) }

sourceSets {
  main {
    java { srcDir(genGrammarDir) }
    // antlr { setSrcDirs(listOf(project.projectDir.resolve("src/main/antlr4"))) }
  }
  // test { antlr { setSrcDirs(listOf(project.projectDir.resolve("src/test/antlr4"))) } }
}

/*
// Gradle's implementation of the antlr plugin creates a configuration called "antlr" and lets
// the "api" configuration extend "antlr", which leaks the antlr tool and runtime plus dependencies
// to all users of this project. So do not let "api" extend from "antlr".
configurations.api.get().setExtendsFrom(listOf())

tasks.named<AntlrTask>("generateGrammarSource").configure {
  arguments.add("-no-listener")
  arguments.add("-package")
  arguments.add("org.projectnessie.nessie.cli.grammar")
}

tasks.named<AntlrTask>("generateTestGrammarSource").configure {
  arguments.add("-no-listener")
  arguments.add("-package")
  arguments.add("org.projectnessie.nessie.cli.completion.test")
}
*/
tasks.withType<Checkstyle>().configureEach {
  // Cannot exclude build/ as a "general coguration", because the Checstyle task creates an
  // ant script behind the scenes, and that only supports "string" pattern matching using.
  // The base directores are the source directories, so all patterns match against paths
  // relative to a source-directory, not against full path names, not even relative to the current
  // project.
  exclude("org/projectnessie/nessie/cli/gr/*")
  // exclude("org/projectnessie/nessie/cli/grammar/*")
  // exclude("org/projectnessie/nessie/cli/completion/test/*")
}

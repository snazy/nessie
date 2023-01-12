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

import org.jetbrains.kotlin.gradle.targets.js.webpack.KotlinWebpackConfig

plugins {
  alias(libs.plugins.kotlin.js)
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Web UI"

dependencies {
  api(enforcedPlatform(libs.kotlin.wrappers.bom))
  implementation("org.jetbrains.kotlin-wrappers:kotlin-emotion")
  implementation("org.jetbrains.kotlin-wrappers:kotlin-react")
  implementation("org.jetbrains.kotlin-wrappers:kotlin-react-dom")
  implementation("org.jetbrains.kotlin-wrappers:kotlin-react-router-dom")
  implementation("org.jetbrains.kotlin-wrappers:kotlin-react-redux")
  implementation("org.jetbrains.kotlin-wrappers:kotlin-redux")
  implementation("org.jetbrains.kotlin-wrappers:kotlin-mui")
  implementation("org.jetbrains.kotlin-wrappers:kotlin-mui-icons")
  implementation("org.jetbrains.kotlin-wrappers:kotlin-tanstack-query-core")
  implementation("org.jetbrains.kotlin-wrappers:kotlin-tanstack-react-query")
  implementation("org.jetbrains.kotlin-wrappers:kotlin-web")
  implementation(npm("@emotion/styled", "11.10.6"))
  implementation(npm("@mui/x-data-grid", "5.17.26"))

  // see https://kotlinlang.org/docs/js-react.html#use-an-external-rest-api
  implementation("org.jetbrains.kotlinx:kotlinx-serialization-json")

  testImplementation("org.jetbrains.kotlin:kotlin-test-js")
}

kotlin {
  js(IR) {
    binaries.executable()

    compilations.all {
      kotlinOptions {
        moduleKind = "commonjs"
        sourceMap = true
        sourceMapEmbedSources = "always"
        metaInfo = true
      }
    }

    compilations.named("main") { compileTaskProvider.configure { kotlinOptions.main = "call" } }

    //    compilations.named("test") {
    //      applyKarma()
    //    }

    moduleName = project.name

    browser {
      commonWebpackConfig {
        devServer.run {
          //
        }
        mode =
          if (project.hasProperty("release")) KotlinWebpackConfig.Mode.PRODUCTION
          else KotlinWebpackConfig.Mode.DEVELOPMENT
      }
    }

    nodejs {
      this.runTask {
        //
      }
      this.distribution {
        //
      }
      this.testTask {
        //
      }
    }
    useCommonJs()
  }
}

val browserDistribution = tasks.named("browserDistribution")

val uiJar by
  tasks.registering(Jar::class) {
    dependsOn(browserDistribution)
    from(browserDistribution.get().outputs.files)
  }

val ui by
  configurations.registering {
    isCanBeConsumed = true
    isCanBeResolved = false
  }

artifacts { add(ui.name, uiJar) }

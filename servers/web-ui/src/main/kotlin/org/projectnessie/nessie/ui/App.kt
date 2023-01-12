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

package org.projectnessie.nessie.ui

import csstype.ClassName
import csstype.Color
import emotion.react.css
import mui.material.CssBaseline
import mui.material.Grid
import react.VFC
import react.dom.html.ReactHTML.a
import react.dom.html.ReactHTML.div
import react.dom.html.ReactHTML.h1
import react.dom.html.ReactHTML.h2
import react.dom.html.ReactHTML.img
import react.dom.html.ReactHTML.span
import react.useState
import tanstack.query.core.QueryClient
import tanstack.react.query.QueryClientProvider
import web.location.location
import xs

val configMode = js("CONFIG_MODE")

val Root = VFC {
  div {
    div {
      h1 {
        a {
          href = "#"
          img {
            alt = ""
            src = "/logo.svg"
            width = 50.toDouble()
            height = 50.toDouble()
            className = ClassName("d-inline-block align-top")
          }
        }
        +"Nessie"
        if (configMode == "development") {
          span {
            css { color = Color("red") }
            +" development mode"
          }
        }
      }
    }
  }
}

val Loading = VFC {
  Grid {
    item = true
    xs = 12

    img { src = "spinner.gif" }
    +" Loading..."
  }
}

fun MainTable(defaultBranch: String) = VFC {
  Grid {
    container = true

    val (currentReference, setCurrentReference) = useState(defaultBranch)

    Grid {
      item = true
      xs = 2

      div { h2 { +"Branches And Tags" } }
      References(currentReference, setCurrentReference)()
    }

    Grid {
      item = true
      xs = 10

      Grid {
        container = true

        Grid {
          item = true
          xs = 12

          Grid {
            container = true

            Grid {
              item = true
              xs = 4

              div { h2 { +"Namespaces" } }
              Namespaces(currentReference)()
            }

            Grid {
              item = true
              xs = 8

              div { h2 { +"Contents" } }
              Contents(currentReference)()
            }
          }
        }

        Grid {
          item = true
          xs = 12

          div { h2 { +"Commits" } }
          CommitLog(currentReference)()
        }
      }
    }
  }
}

val queryClient = QueryClient()

val App = VFC {
  QueryClientProvider {
    initAppRootUrlFromLocation(location)

    client = queryClient

    CssBaseline()

    Root()

    VFC {
      val nessieConfiguration = useNessieConfiguration()

      if (nessieConfiguration == null) {
        Loading()
      } else {
        MainTable(nessieConfiguration.defaultBranch)()
      }
    }()
  }
}

// val mainScope = MainScope()
// val AppX =
//  VFC {
//    RouterProvider {
//      var defaultBranch: String by useState("main")
//
//      mainScope.launch {
//        defaultBranch = fetchDefaultBranch()
//      }
//
//      router =
//        createBrowserRouter(
//          arrayOf(
//            jso {
//              path = "/"
//              element = Root.create()
//
//              children =
//                arrayOf(
//                  jso {
//                    index = true
//                    //element = Navigate.create() { to = "/tree" }
//                  },
//                  jso {
//                    path = "tree"
//                    children =
//                      arrayOf(
//                        jso {
//                          index = true
//                          element = Navigate.create() { to = defaultBranch }
//                        },
//                        jso {
//                          path = ":branch"
//                          element = BranchView.create()
//                        },
//                        jso { path = ":branch/*" },
//                        jso { path = "*" }
//                      )
//                  },
//                  jso {
//                    path = "content"
//                    children =
//                      arrayOf(
//                        jso { index = true },
//                        jso { path = ":branch" },
//                        jso { path = ":branch/*" },
//                        jso { path = "*" }
//                      )
//                  },
//                  jso {
//                    path = "commits"
//                    children =
//                      arrayOf(
//                        jso { index = true },
//                        jso { path = ":branch" },
//                        jso { path = ":branch/:commit-id" },
//                        jso { path = "*" }
//                      )
//                  },
//                  jso {
//                    path = "notfound"
//                    element = NotFound.create()
//                  }
//                )
//            }
//          )
//        )
//    }
//  }

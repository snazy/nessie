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

import csstype.em
import emotion.react.css
import mui.material.Grid
import react.VFC
import react.dom.html.ReactHTML
import xs

fun contentsPage(ref: String, token: String?): VFC = VFC {
  val entries = useEntries(ref, pageToken = token)
  if (entries == null) {
    Loading()
  } else {
    console.info(
      "Loaded ${entries.entries.size} content entries (for contents), hasMore: ${entries.hasMore}, current: ${token}, next: ${entries.token}"
    )
    for (entry in entries.entries) {
      if (entry.type != "NAMESPACE") {
        Grid {
          item = true
          xs = 12

          when (entry.type) {
            "ICEBERG_TABLE" -> {
              ReactHTML.img {
                src = "iceberg.png"
                css { height = 1.em }
              }
              +" table"
            }
            "ICEBERG_VIEW" -> {
              ReactHTML.img {
                src = "iceberg.png"
                css { height = 1.em }
              }
              +" view"
            }
            "DELTA_LAKE_TABLE" -> ReactHTML.img { src = "delta.png" }
            else -> {}
          }
          +" ${contentKeyAsString(entry.name)}"
        }
      }
    }
    if (entries.hasMore) {
      // TODO can we make the function call conditional and trigger only when the information
      //  becomes visible?
      contentsPage(ref, entries.token)()
    }
  }
}

fun Contents(reference: String) = VFC {
  Grid {
    container = true

    contentsPage(reference, null)()
  }
}

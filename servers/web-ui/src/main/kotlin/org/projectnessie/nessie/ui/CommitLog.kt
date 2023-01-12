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

import mui.material.Grid
import react.VFC
import react.dom.html.ReactHTML.div
import xs

fun commitLogPage(ref: String, token: String?): VFC = VFC {
  val logs = useCommitLog(ref, maxRecords = 20, pageToken = token)
  if (logs == null) {
    Loading()
  } else {
    console.info(
      "Loaded ${logs.logEntries.size} commit log entries, hasMore: ${logs.hasMore}, current: ${token}, next: ${logs.token}"
    )
    for (logEntry in logs.logEntries) {
      Grid {
        item = true
        xs = 1

        val id = logEntry.commitMeta.hash!!
        copyClipboard(id, "${id.substring(0, 12)}...")()
      }

      Grid {
        item = true
        xs = 2

        +logEntry.commitMeta.commitTime
        // TODO +"${Date(Date.parse(logEntry.commitMeta.commitTime!!))}"
      }

      Grid {
        item = true
        xs = 9

        +logEntry.commitMeta.message
      }
    }
    if (logs.hasMore) {
      // TODO can we make the function call conditional and trigger only when the information
      //  becomes visible?
      commitLogPage(ref, logs.token)()
    }
  }
}

fun CommitLog(reference: String) = VFC {
  Grid {
    container = true

    commitLogPage(reference, null)()
  }
}

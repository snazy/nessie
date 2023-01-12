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
import react.StateSetter
import react.VFC
import react.dom.html.ReactHTML.b
import xs

fun referencesPage(
  currentReference: String,
  setCurrentReference: StateSetter<String>,
  token: String?
): VFC = VFC {
  val references = useReferences(pageToken = token)
  if (references == null) {
    Loading()
  } else {
    console.info(
      "Loaded ${references.references.size} references, hasMore: ${references.hasMore}, current: ${token}, next: ${references.token}"
    )
    for (reference in references.references) {
      Grid {
        item = true
        xs = 12

        if (currentReference == reference.name) {
          b { +reference.name }
        } else {
          +reference.name
        }

        onClick = { setCurrentReference.invoke(reference.name) }
      }
    }
    if (references.hasMore) {
      // TODO can we make the function call conditional and trigger only when the information
      //  becomes visible?
      referencesPage(currentReference, setCurrentReference, references.token)()
    }
  }
}

fun References(currentReference: String, setCurrentReference: StateSetter<String>): VFC = VFC {
  Grid {
    container = true

    referencesPage(currentReference, setCurrentReference, null)()
  }
}

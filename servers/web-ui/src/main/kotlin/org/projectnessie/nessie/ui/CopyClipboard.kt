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
import csstype.Cursor
import js.core.jso
import kotlin.time.Duration.Companion.seconds
import react.VFC
import react.dom.html.ReactHTML
import react.useState
import web.navigator.navigator
import web.timers.setTimeout

fun copyClipboard(fullText: String, displayText: String? = null): VFC = VFC {
  ReactHTML.span {
    title = fullText
    ReactHTML.span {
      val (copyValue, setCopyValue) = useState("\ue14d")

      style = jso { cursor = Cursor.copy }
      title = "Copy commit ID to clipboard"
      onClick = {
        navigator.clipboard.writeText(fullText)
        setCopyValue("\ue876")
        setTimeout(1.seconds) { setCopyValue("\ue14d") }
      }
      className = ClassName("material-icons")

      +copyValue
    }
    +(displayText ?: fullText)
  }
}

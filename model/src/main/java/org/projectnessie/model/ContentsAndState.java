/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

/**
 * Contains both the global and per-branch (or per-tag) state of a table, which are consistent to
 * each other.
 */
@Schema(
    type = SchemaType.OBJECT,
    title = "Contents its global state",
    description =
        " Contains both the global and per-branch (or per-tag) state of a table, which are "
            + "consistent to each other.")
@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableContentsAndState.class)
@JsonDeserialize(as = ImmutableContentsAndState.class)
public interface ContentsAndState {
  @NotNull
  Contents getContents();

  /** The corresponding current global state for the {@link #getContents()} datalake object. */
  GlobalContents getGlobalContents();
}

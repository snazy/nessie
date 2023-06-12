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
package org.projectnessie.quarkus.cli;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;

@Value.Immutable
@JsonSerialize(as = ImmutableCheckContentEntry.class)
@JsonDeserialize(as = ImmutableCheckContentEntry.class)
public interface CheckContentEntry {

  String getStatus();

  ContentKey getKey();

  @JsonInclude(NON_EMPTY)
  @Nullable
  Content getContent();

  @JsonInclude(NON_EMPTY)
  @Nullable
  String getErrorMessage();

  @JsonInclude(NON_EMPTY)
  @Nullable
  String getExceptionStackTrace();
}

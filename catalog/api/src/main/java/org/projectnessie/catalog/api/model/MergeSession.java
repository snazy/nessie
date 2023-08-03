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
package org.projectnessie.catalog.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * {@link MergeSession Merge session} objects returned in a {@link MergeOutcome} must be passed
 * as-is in the next {@link MergeRequest#getSession() merge request}.
 *
 * <p>The contents of the fields in this object must neither be interpreted nor modified.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableMergeSession.class)
@JsonDeserialize(as = ImmutableMergeSession.class)
public interface MergeSession {
  @Value.Parameter(order = 1)
  String sessionId();

  @Value.Parameter(order = 2)
  String signature();
}

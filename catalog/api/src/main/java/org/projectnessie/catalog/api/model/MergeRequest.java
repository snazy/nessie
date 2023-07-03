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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import org.immutables.value.Value;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.Validation;

@Value.Immutable
@JsonSerialize(as = ImmutableMergeRequest.class)
@JsonDeserialize(as = ImmutableMergeRequest.class)
public interface MergeRequest {
  static ImmutableMergeRequest.Builder builder() {
    return ImmutableMergeRequest.builder();
  }

  /**
   * Merge sessions are used to identify multiple merge-attempts round-trips.
   *
   * <p>A user request to merge some source onto some target, including all round-trips to resolve
   * conflicts, belongs to one <em>merge session</em>.
   *
   * <p>Merge sessions are exclusively assigned by the endpoints. Requests must include the {@link
   * MergeSession} returned from the previous requests for the same logical merge.
   */
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  MergeSession getSession();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  CommitMeta getCommitMeta();

  @NotBlank
  @jakarta.validation.constraints.NotBlank
  @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
  @jakarta.validation.constraints.Pattern(
      regexp = Validation.HASH_REGEX,
      message = Validation.HASH_MESSAGE)
  String getFromHash();

  @NotBlank
  @jakarta.validation.constraints.NotBlank
  @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
  @jakarta.validation.constraints.Pattern(
      regexp = Validation.REF_NAME_REGEX,
      message = Validation.REF_NAME_MESSAGE)
  String getFromRefName();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  List<MergeKeyBehavior> getKeyMergeModes();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  MergeBehavior getDefaultKeyMergeMode();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Value.Default
  default Boolean isDryRun() {
    return Boolean.FALSE;
  }
}

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
package org.projectnessie.api.v1;

import static org.projectnessie.api.v1.params.DiffParams.HASH_OPTIONAL_REGEX;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.Validation;

public interface DiffApi {

  /**
   * Returns a list of diff values that show the difference between two given references.
   *
   * @return A list of diff values that show the difference between two given references.
   */
  DiffResponse getDiff(
      @NotNull
          @jakarta.validation.constraints.NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = Validation.REF_NAME_REGEX,
              message = Validation.REF_NAME_MESSAGE)
          String fromRefWithHash,
      @Nullable
          @jakarta.annotation.Nullable
          @Pattern(regexp = HASH_OPTIONAL_REGEX, message = Validation.HASH_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = HASH_OPTIONAL_REGEX,
              message = Validation.HASH_MESSAGE)
          String toRefWithHash)
      throws NessieNotFoundException;
}

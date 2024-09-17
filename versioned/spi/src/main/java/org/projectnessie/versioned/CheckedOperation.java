/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned;

import org.immutables.value.Value;
import org.projectnessie.model.Operation;
import org.projectnessie.services.authz.AccessCheckParams;

@Value.Immutable
@Value.Style(allParameters = true)
public interface CheckedOperation {
  Operation operation();

  AccessCheckParams accessCheckParams();

  static CheckedOperation checkedOperation(
      Operation operation, AccessCheckParams accessCheckParams) {
    return ImmutableCheckedOperation.of(operation, accessCheckParams);
  }
}

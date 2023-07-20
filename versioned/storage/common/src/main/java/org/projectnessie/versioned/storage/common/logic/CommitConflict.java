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
package org.projectnessie.versioned.storage.common.logic;

import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;

@Value.Immutable
public interface CommitConflict {
  @Value.Parameter(order = 1)
  StoreKey key();

  @Value.Parameter(order = 2)
  ConflictType conflictType();

  @Value.Parameter(order = 3)
  @Nullable
  @jakarta.annotation.Nullable
  CommitOp op();

  @Value.Parameter(order = 4)
  @Nullable
  @jakarta.annotation.Nullable
  CommitOp existing();

  @Value.Parameter(order = 5)
  @Nullable
  @jakarta.annotation.Nullable
  StoreKey renameTo();

  static CommitConflict commitConflict(
      StoreKey key,
      ConflictType conflictType,
      @Nullable @jakarta.annotation.Nullable CommitOp op,
      @Nullable @jakarta.annotation.Nullable StoreKey renameTo) {
    return commitConflict(key, conflictType, op, null, renameTo);
  }

  static CommitConflict commitConflict(
      StoreKey key,
      ConflictType conflictType,
      @Nullable @jakarta.annotation.Nullable CommitOp op,
      @Nullable @jakarta.annotation.Nullable CommitOp existing,
      @Nullable @jakarta.annotation.Nullable StoreKey renameTo) {
    return ImmutableCommitConflict.of(key, conflictType, op, existing, renameTo);
  }

  enum ConflictType {
    /** The key exists, but is expected to not exist. */
    KEY_EXISTS,

    /** The key does not exist, but is expected to exist. */
    KEY_DOES_NOT_EXIST,

    /** Payload of existing and expected differ. */
    PAYLOAD_DIFFERS,

    /** Content IDs of existing and expected content differs. */
    CONTENT_ID_DIFFERS,

    /** Values of existing and expected content differs. */
    VALUE_DIFFERS
  }
}

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
package org.projectnessie.versioned.storage.transitional;

import java.util.function.IntConsumer;
import org.projectnessie.versioned.storage.common.persist.Persist;

public interface TransitionalPersist extends Persist {
  void transition(
      int objStoreBatchSize,
      int createReferencesParallelism,
      IntConsumer commitsTransitioned,
      Runnable referenceCreated);

  TransitionalStatus status();

  enum TransitionalStatus {
    INTERMEDIATE,
    TRANSITIONING,
    EVENTUAL,
    FAILED
  }
}

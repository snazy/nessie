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
package org.projectnessie.versioned.storage.foundationdb;

import jakarta.annotation.Nonnull;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;

final class FoundationDBPersistFactory implements PersistFactory {
  private final FoundationDBBackend backend;

  FoundationDBPersistFactory(FoundationDBBackend backend) {
    this.backend = backend;
  }

  @Override
  @Nonnull
  public Persist newPersist(@Nonnull StoreConfig config) {
    return new FoundationDBPersist(backend, config);
  }
}

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
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;

public class FoundationDBBackendFactory implements BackendFactory<FoundationDBBackendConfig> {

  public static final String NAME = "BigTable";

  @Override
  @Nonnull
  public String name() {
    return NAME;
  }

  @Override
  @Nonnull
  public FoundationDBBackendConfig newConfigInstance() {
    // Note: this method should not be called and will throw because dataClient is not set.
    // FoundationDBBackendConfig instances cannot be constructed using this method.
    return FoundationDBBackendConfig.builder().build();
  }

  @Override
  @Nonnull
  public Backend buildBackend(@Nonnull FoundationDBBackendConfig config) {
    return new FoundationDBBackend(config);
  }
}

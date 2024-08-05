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
import java.util.Optional;
import java.util.Set;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FoundationDBBackend implements Backend {
  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBBackend.class);
  private final FoundationDBSingleton.FoundationDBHandle handle;
  private final FoundationDBBackendConfig config;

  public FoundationDBBackend(@Nonnull FoundationDBBackendConfig config) {
    this.config = config;
    this.handle =
        FoundationDBSingleton.openCluster(
            config
                .clusterFileContents()
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            "Mandatory 'clusterFileContents' is missing")));
  }

  @Nonnull
  public FoundationDBBackendConfig config() {
    return config;
  }

  @Override
  @Nonnull
  public PersistFactory createFactory() {
    return new FoundationDBPersistFactory(this);
  }

  @Override
  public void close() {
    this.handle.close();
  }

  @Override
  public Optional<String> setupSchema() {
    return Optional.empty();
  }

  @Override
  public void eraseRepositories(Set<String> repositoryIds) {
    throw new UnsupportedOperationException();
  }
}

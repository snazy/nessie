/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned.impl;

import java.util.List;
import java.util.Optional;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.impl.ImmutableTieredVersionStoreConfig.Builder;
import org.projectnessie.versioned.impl.InternalBranch.UpdateState;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.util.BackoffConfig;

@Immutable
public interface TieredVersionStoreConfig {

  /**
   * The number of <em>re</em>tries for {@link TieredVersionStore#commit(BranchName, Optional, Object, List)}.
   */
  int DEFAULT_COMMIT_RETRIES = 4;
  /**
   * The number of <em>re</em>tries for
   * {@link InternalBranch.UpdateState#collapseIntentionLog(UpdateState, Store, InternalBranch, BackoffConfig)}.
   */
  int DEFAULT_P2_COMMIT_RETRIES = 4;

  /**
   * Whether to block on collapsing the InternalBranch commit log before returning valid L1s.
   * @return {@code true} to block on collapsing the InternalBranch commit log
   */
  @Default
  default boolean waitOnCollapse() {
    return false;
  }

  /**
   * The backoff-configuration used during {@link TieredVersionStore#commit(BranchName, Optional, Object, List)}.
   * @return backoff-config for commits
   */
  @Default
  default BackoffConfig commitBackoff() {
    return BackoffConfig.builder()
        .retries(DEFAULT_COMMIT_RETRIES)
        .build();
  }

  /**
   * The backoff-configuration used during
   * {@link InternalBranch.UpdateState#collapseIntentionLog(UpdateState, Store, InternalBranch, BackoffConfig)}.
   * @return backoff-config used when collapsing the branch-commit-log
   */
  @Default
  default BackoffConfig p2CommitBackoff() {
    return BackoffConfig.builder()
        .retries(DEFAULT_P2_COMMIT_RETRIES)
        .build();
  }

  @Default
  default CacheConfig cache() {
    return CacheConfig.builder().build();
  }

  @Immutable
  interface CacheConfig {

    int DEFAULT_SIZE = 1000;

    /**
     * Whether the cache is enabled, defaults to {@code false}.
     * @return Cache-enabled
     */
    @Default
    default boolean enabled() {
      return false;
    }

    /**
     * Maximum number of items in the cache, defaults to {@value DEFAULT_SIZE}.
     * @return Maximum number of items in the cache.
     */
    @Default
    default int maxSize() {
      return DEFAULT_SIZE;
    }

    /**
     * Whether the cache records statistics, defaults to {@code true}.
     * @return Cache-stats enabled
     */
    @Default
    default boolean recordStats() {
      return true;
    }

    static ImmutableCacheConfig.Builder builder() {
      return ImmutableCacheConfig.builder();
    }
  }

  static Builder builder() {
    return ImmutableTieredVersionStoreConfig.builder();
  }

  /**
   * Configuration for <em>tests</em>: waits for "collapse" and no tiered-version-store-cache.
   * @return configuration for tests
   */
  static TieredVersionStoreConfig testConfig() {
    return builder().waitOnCollapse(true).build();
  }
}

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
package org.projectnessie.server.config;

import java.time.Duration;

import org.eclipse.microprofile.config.inject.ConfigProperty;

public interface TieredVersionStoreConfig {

  /**
   * Whether calls against the tiered-backend-store are traced with
   * OpenTracing/OpenTelemetry (Jaeger), enabled by default.
   */
  @ConfigProperty(name = "trace.enable", defaultValue = "true")
  boolean isTracingEnabled();

  /**
   * The maximum number of retries, a value of {@code 0}, means "unlimited" or until {@link
   * #getCommitBackoffMaxTime()} triggers. Default value os {@code 5}. If both {@link #getCommitBackoffRetries()}
   * and {@link #getCommitBackoffMaxTime()} would never trigger, no retries are allowed.
   *
   * @return max time to retry.
   */
  @ConfigProperty(name = "commit-backoff.retries", defaultValue = "5")
  int getCommitBackoffRetries();

  /**
   * The maximum amount of time to retry, a value of {@code 0}, which is the default, means
   * "unlimited" or until {@link #getCommitBackoffRetries()} triggers. If both {@link #getCommitBackoffRetries()}
   * and {@link #getCommitBackoffMaxTime()} would never trigger, no retries are allowed.
   *
   * @return max time to retry.
   */
  @ConfigProperty(name = "commit-backoff.max-time", defaultValue = "0")
  Duration getCommitBackoffMaxTime();

  @ConfigProperty(name = "commit-backoff.retry-sleep", defaultValue = "0")
  Duration getCommitBackoffRetrySleep();

  /**
   * The commit-backoff multiplier. A value greater than 1 enables exponential backoff with
   * this multiplier. Formula: {@code time-between-retries = retry-sleep * retry ^ multiplier}.
   * @return backoff-multiplier, defaults to {@code 1}
   */
  @ConfigProperty(name = "commit-backoff.multiplier", defaultValue = "1")
  double getCommitBackoffMultiplier();

  /**
   * Jitter for retries. A value greater than 0 and less than 1 enables the retry-sleep-time
   * jitter. Formula: {@code retry-sleep -= random(0..jitter) * retry-sleep }
   *
   * @return jitter (factor), defaults to {@code 1}
   */
  @ConfigProperty(name = "commit-backoff.jitter", defaultValue = "1")
  double getCommitBackoffJitter();

  /**
   * The maximum number of retries, a value of {@code 0}, means "unlimited" or until {@link
   * #getP2CommitBackoffMaxTime()} triggers. Default value os {@code 5}. If both {@link #getP2CommitBackoffRetries()}
   * and {@link #getP2CommitBackoffMaxTime()} would never trigger, no retries are allowed.
   *
   * @return max time to retry.
   */
  @ConfigProperty(name = "p2-commit-backoff.retries", defaultValue = "5")
  int getP2CommitBackoffRetries();

  /**
   * The maximum amount of time to retry, a value of {@code 0}, which is the default, means
   * "unlimited" or until {@link #getP2CommitBackoffRetries()} triggers. If both {@link #getP2CommitBackoffRetries()}
   * and {@link #getP2CommitBackoffMaxTime()} would never trigger, no retries are allowed.
   *
   * @return max time to retry.
   */
  @ConfigProperty(name = "p2-commit-backoff.max-time", defaultValue = "0")
  Duration getP2CommitBackoffMaxTime();

  @ConfigProperty(name = "p2-commit-backoff.retry-sleep", defaultValue = "0")
  Duration getP2CommitBackoffRetrySleep();

  /**
   * The commit-backoff multiplier. A value greater than 1 enables exponential backoff with
   * this multiplier. Formula: {@code time-between-retries = retry-sleep * retry ^ multiplier}.
   * @return backoff-multiplier, defaults to {@code 1}
   */
  @ConfigProperty(name = "p2-commit-backoff.multiplier", defaultValue = "1")
  double getP2CommitBackoffMultiplier();

  /**
   * Jitter for retries. A value greater than 0 and less than 1 enables the retry-sleep-time
   * jitter. Formula: {@code retry-sleep -= random(0..jitter) * retry-sleep }
   *
   * @return jitter (factor), defaults to {@code 1}
   */
  @ConfigProperty(name = "p2-commit-backoff.jitter", defaultValue = "1")
  double getP2CommitBackoffJitter();

  /**
   * Whether the tiered-version-store cache is enabled, defaults to {@code false}.
   * @return {@code true} to enable the cache
   */
  @ConfigProperty(name = "cache.enabled", defaultValue = "false")
  boolean isCacheEnabled();

  /**
   * Whether the cache records statistics, defaults to {@code true}.
   */
  @ConfigProperty(name = "cache.record-stats", defaultValue = "true")
  boolean isCacheRecordStats();

  /**
   * Maximum number of cached items in the cache, defaults to {@code 1000}.
   */
  @ConfigProperty(name = "cache.max-size", defaultValue = "1000")
  int getCacheMaxSize();
}

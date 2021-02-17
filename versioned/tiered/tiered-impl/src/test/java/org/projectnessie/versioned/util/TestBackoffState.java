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
package org.projectnessie.versioned.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

// BackoffState.retry declares `throws E` (a generic) and this test implies a runtime-exception,
// so the `throws Exception` clauses below are technically superfluous. However, javac fails to
// compile this test, if `throws Exception` is missing.
@SuppressWarnings("RedundantThrows")
class TestBackoffState {

  @Test
  void testNoopBackoff() {
    AtomicBoolean timeCalled = new AtomicBoolean();

    BackoffState state = create(BackoffConfig.NONE,
        () -> {
          // Allow the BackoffState to fetch the start-time, but no more invocations.
          assertFalse(timeCalled.getAndSet(true));
          return 0L;
        },
        d -> fail());

    assertEquals(0, state.getTry());
    assertNoRetry(state);
    assertEquals(0, state.getTry());
    assertNoRetry(state);
    assertEquals(0, state.getTry());
  }

  @Test
  void testRetriesNoSleep() throws Exception {
    AtomicBoolean timeCalled = new AtomicBoolean();

    BackoffState state = create(BackoffConfig.builder().retries(3).build(),
        () -> {
          // Allow the BackoffState to fetch the start-time, but no more invocations.
          assertFalse(timeCalled.getAndSet(true));
          return 0L;
        },
        d -> fail());

    for (int i = 0; i < 3; i++) {
      assertEquals(i, state.getTry());
      state.retry(x -> fail());
      assertEquals(i + 1, state.getTry());
    }

    assertNoRetry(state);
    assertEquals(3, state.getTry());
    assertNoRetry(state);
    assertEquals(3, state.getTry());
  }

  @Test
  void testRetriesWithSleep() throws Exception {
    AtomicInteger sleepInvocations = new AtomicInteger();

    BackoffState state = create(BackoffConfig.builder().retries(3).retrySleep(Duration.ofNanos(10)).build(),
        () -> 0L,
        d -> {
          sleepInvocations.incrementAndGet();
          assertEquals(10, d.toNanos());
        });

    for (int i = 0; i < 3; i++) {
      assertEquals(i, sleepInvocations.get());
      assertEquals(i, state.getTry());
      state.retry(x -> fail());
      assertEquals(i + 1, state.getTry());
    }

    assertEquals("after 3 retries", assertNoRetry(state));
    assertEquals(3, state.getTry());
    assertEquals("after 3 retries", assertNoRetry(state));
    assertEquals(3, state.getTry());
    assertEquals(3, sleepInvocations.get());
  }

  @Test
  void testUnlimitedRetriesWithMaxTime() throws Exception {
    long[] currentTime = new long[1];

    BackoffState state = create(BackoffConfig.builder().maxTime(Duration.ofSeconds(1)).build(),
        () -> currentTime[0],
        d -> fail());

    for (int i = 0; i < 20; i++) {
      assertEquals(i, state.getTry());
      state.retry(x -> fail());
      assertEquals(i + 1, state.getTry());
    }

    currentTime[0] = TimeUnit.SECONDS.toNanos(1) - 1;

    for (int i = 20; i < 40; i++) {
      assertEquals(i, state.getTry());
      state.retry(x -> fail());
      assertEquals(i + 1, state.getTry());
    }

    currentTime[0] = TimeUnit.SECONDS.toNanos(1);

    assertEquals("after PT1S", assertNoRetry(state));
    assertEquals(40, state.getTry());
    assertEquals("after PT1S", assertNoRetry(state));
    assertEquals(40, state.getTry());

    currentTime[0] = TimeUnit.SECONDS.toNanos(1) + 1;

    assertEquals("after PT1.000000001S", assertNoRetry(state));
    assertEquals(40, state.getTry());
    assertEquals("after PT1.000000001S", assertNoRetry(state));
    assertEquals(40, state.getTry());
  }

  @Test
  void testRetriesWithMaxTimeQuick() throws Exception {
    long[] currentTime = new long[1];

    BackoffState state = create(BackoffConfig.builder().retries(3).maxTime(Duration.ofSeconds(1)).build(),
        () -> currentTime[0],
        d -> fail());

    currentTime[0] = TimeUnit.SECONDS.toNanos(1) - 1;

    for (int i = 0; i < 3; i++) {
      assertEquals(i, state.getTry());
      state.retry(x -> fail());
      assertEquals(i + 1, state.getTry());
    }

    assertEquals("after 3 retries", assertNoRetry(state));
    assertEquals(3, state.getTry());
    assertEquals("after 3 retries", assertNoRetry(state));
    assertEquals(3, state.getTry());
  }

  @Test
  void testRetriesWithMaxTimeSlow() throws Exception {
    long[] currentTime = new long[1];

    BackoffState state = create(BackoffConfig.builder().retries(3).maxTime(Duration.ofSeconds(1)).build(),
        () -> currentTime[0],
        d -> fail());

    currentTime[0] = TimeUnit.SECONDS.toNanos(1) - 1;

    assertEquals(0, state.getTry());
    state.retry(x -> fail());
    assertEquals(1, state.getTry());
    state.retry(x -> fail());
    assertEquals(2, state.getTry());
    currentTime[0] = TimeUnit.SECONDS.toNanos(1);
    assertEquals("after PT1S", assertNoRetry(state));
    assertEquals(2, state.getTry());
    assertEquals("after PT1S", assertNoRetry(state));
    assertEquals(2, state.getTry());
  }

  @Test
  void testRetriesWithMaxTimeSlowWIthSleep() throws Exception {
    long[] currentTime = new long[1];
    AtomicInteger sleepInvocations = new AtomicInteger();

    BackoffState state = create(BackoffConfig.builder().retries(3).maxTime(Duration.ofSeconds(1)).retrySleep(Duration.ofNanos(10L)).build(),
        () -> currentTime[0],
        d -> {
          sleepInvocations.incrementAndGet();
          assertEquals(10, d.toNanos());
        });

    currentTime[0] = TimeUnit.SECONDS.toNanos(1) - 1;

    assertEquals(0, state.getTry());
    assertEquals(0, sleepInvocations.get());
    state.retry(x -> fail());
    assertEquals(1, sleepInvocations.get());
    assertEquals(1, state.getTry());
    state.retry(x -> fail());
    assertEquals(2, sleepInvocations.get());
    assertEquals(2, state.getTry());
    currentTime[0] = TimeUnit.SECONDS.toNanos(1);
    assertEquals("after PT1S", assertNoRetry(state));
    assertEquals(2, state.getTry());
    assertEquals("after PT1S", assertNoRetry(state));
    assertEquals(2, state.getTry());
    assertEquals(2, sleepInvocations.get());
  }

  @Test
  void testExponentialBackoff() throws Exception {
    long[] currentTime = new long[1];
    AtomicLong sleepNanos = new AtomicLong();

    BackoffState state = create(BackoffConfig.builder()
            .maxTime(Duration.ofSeconds(1)).retrySleep(Duration.ofNanos(10L)).multiplier(2d)
            .build(),
        () -> currentTime[0],
        d -> {
          sleepNanos.set(d.toNanos());
          currentTime[0] += d.toNanos();
        });

    long expectedSleep = 10L;
    for (int i = 0; i <= 26; i++) {
      state.retry(x -> fail());
      assertEquals(expectedSleep, sleepNanos.get());
      expectedSleep *= 2;
    }
    assertNoRetry(state);
  }

  @Test
  void testJitter() throws Exception {
    long[] currentTime = new long[1];
    AtomicLong sleepNanos = new AtomicLong();

    BackoffState state = create(BackoffConfig.builder()
            .maxTime(Duration.ofSeconds(1)).retrySleep(Duration.ofNanos(10000L)).jitter(0.1d)
            .build(),
        () -> currentTime[0],
        d -> {
          sleepNanos.set(d.toNanos());
          currentTime[0] += d.toNanos();
        });

    while (currentTime[0] < TimeUnit.SECONDS.toNanos(1)) {
      // With the configured jitter of 0.1, sleep durations between 9000..10000 are valid.
      state.retry(x -> fail());
      assertThat(sleepNanos.get(), allOf(lessThanOrEqualTo(10000L), greaterThanOrEqualTo(9000L)));
    }
    assertNoRetry(state);
  }

  static Double[] invalidJitters() {
    return new Double[]{-1d, 0d, 1.1d, 2d};
  }

  // invalid jitters (<= 0 or > 1) fall back to 1
  @ParameterizedTest
  @MethodSource("invalidJitters")
  void testJitterInvalid(double invalidJitter) throws Exception {
    long[] currentTime = new long[1];
    AtomicLong sleepNanos = new AtomicLong();

    BackoffState state = create(BackoffConfig.builder()
            .maxTime(Duration.ofSeconds(1)).retrySleep(Duration.ofNanos(10000L)).jitter(invalidJitter)
            .build(),
        () -> currentTime[0],
        d -> {
          sleepNanos.set(d.toNanos());
          currentTime[0] += d.toNanos();
        });

    while (currentTime[0] < TimeUnit.SECONDS.toNanos(1)) {
      state.retry(x -> fail());
      assertEquals(10000L, sleepNanos.get());
    }
    assertNoRetry(state);
  }

  static Double[] invalidMultipliers() {
    return new Double[]{0d, 0.9d, -1d};
  }

  // invalid multipliers (< 1) fall back to 1
  @ParameterizedTest
  @MethodSource("invalidMultipliers")
  void testMultiplierInvalid(double invalidMultiplier) throws Exception {
    long[] currentTime = new long[1];
    AtomicLong sleepNanos = new AtomicLong();

    BackoffState state = create(BackoffConfig.builder()
            .maxTime(Duration.ofSeconds(1)).retrySleep(Duration.ofNanos(10000L)).multiplier(invalidMultiplier)
            .build(),
        () -> currentTime[0],
        d -> {
          sleepNanos.set(d.toNanos());
          currentTime[0] += d.toNanos();
        });

    while (currentTime[0] < TimeUnit.SECONDS.toNanos(1)) {
      state.retry(x -> fail());
      assertEquals(10000L, sleepNanos.get());
    }
    assertNoRetry(state);
  }

  static String assertNoRetry(BackoffState state) {
    AtomicReference<String> reason = new AtomicReference<>();
    assertEquals("This is good",
        assertThrows(RuntimeException.class, () -> state.retry(failureReason -> {
          reason.set(failureReason);
          return new RuntimeException("This is good");
        })).getMessage());
    return reason.get();
  }

  static BackoffState create(BackoffConfig config, LongSupplier currentTime, Consumer<Duration> sleeper) {
    return new BackoffState(config) {
      @Override
      long currentTimeNanos() {
        return currentTime.getAsLong();
      }

      @Override
      void sleep(Duration sleep) {
        sleeper.accept(sleep);
      }
    };
  }
}

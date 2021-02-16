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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestBranchCollapseSync {

  ExecutorService executor;

  @BeforeEach
  void createExecutor() {
    executor = Executors.newCachedThreadPool();
  }

  @AfterEach
  void stopExecutor() throws Exception {
    executor.shutdown();
    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
  }

  /**
   * Verifies the simplest scenario: A single branch-collapse-run, no concurrently triggered
   * branch-collapses.
   */
  @Test
  void singleShot() {
    // Latch triggered when the branch-collapse-run finishes
    CountDownLatch stopLatch = new CountDownLatch(1);

    MockExecutor executor = new MockExecutor(
        () -> {},
        stopLatch::countDown
    );

    // Number of handler runs
    AtomicInteger handlerRuns = new AtomicInteger();

    // Create the branch-collapse-sync object
    BranchCollapseSync<String> sync = new BranchCollapseSync<>();

    // Submit the branch-collapse
    sync.submitCollapse(executor, "foo", handlerRuns::incrementAndGet);

    // Wait for the worker to finish
    awaitLatch(stopLatch);

    assertEquals(1, handlerRuns.get());
  }

  /**
   * Test scenario: A branch-collapse is triggered and while it runs, another one is being triggered.
   */
  @Test
  void doubleShot() {
    // Latch triggered when the branch-collapse-run finishes
    CountDownLatch stopLatch = new CountDownLatch(1);

    MockExecutor executor = new MockExecutor(
        () -> {},
        stopLatch::countDown
    );

    // Number of handler runs
    AtomicInteger handlerRuns = new AtomicInteger();

    // Create the branch-collapse-sync object
    BranchCollapseSync<String> sync = new BranchCollapseSync<>();

    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch secondSubmit = new CountDownLatch(1);

    // Submit the 1st branch-collapse
    sync.submitCollapse(executor, "foo", () -> {
      handlerRuns.incrementAndGet();
      started.countDown();
      awaitLatch(secondSubmit);
    });

    // Wait that the branch-collapse-run started
    awaitLatch(started);

    // Submit the 2nd branch-collapse
    sync.submitCollapse(executor, "foo", handlerRuns::incrementAndGet);

    // Let the first-collapse finish
    secondSubmit.countDown();

    // Wait for the worker to finish
    awaitLatch(stopLatch);

    assertEquals(2, handlerRuns.get());
  }

  /**
   * Test scenario: A branch-collapse is triggered and while it runs, another two other ones are
   * being triggered, but only the last one shall run.
   */
  @Test
  void threeShots() {
    // Latch triggered when the branch-collapse-run finishes
    CountDownLatch stopLatch = new CountDownLatch(1);

    MockExecutor executor = new MockExecutor(
        () -> {},
        stopLatch::countDown
    );

    // Number of handler runs
    AtomicInteger handlerRuns = new AtomicInteger();

    // Create the branch-collapse-sync object
    BranchCollapseSync<String> sync = new BranchCollapseSync<>();

    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch secondSubmit = new CountDownLatch(1);

    // Submit the 1st branch-collapse
    sync.submitCollapse(executor, "foo", () -> {
      handlerRuns.incrementAndGet();
      started.countDown();
      awaitLatch(secondSubmit);
    });

    // Wait that the branch-collapse-run started
    awaitLatch(started);

    // Submit the 2nd branch-collapse that must not run
    AtomicBoolean wrongOneTriggered = new AtomicBoolean();
    sync.submitCollapse(executor, "foo", () -> wrongOneTriggered.set(true));

    // Submit the 3nd branch-collapse that must run
    sync.submitCollapse(executor, "foo", handlerRuns::incrementAndGet);

    // Let the first-collapse finish
    secondSubmit.countDown();

    // Wait for the worker to finish
    awaitLatch(stopLatch);

    assertFalse(wrongOneTriggered.get());
    assertEquals(2, handlerRuns.get());
  }

  static void awaitLatch(CountDownLatch latch) {
    try {
      assertTrue(latch.await(10, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  class MockExecutor implements Executor {
    final Runnable beforeRun;
    final Runnable afterRun;

    MockExecutor(Runnable beforeRun, Runnable afterRun) {
      this.beforeRun = beforeRun;
      this.afterRun = afterRun;
    }

    @Override
    public void execute(Runnable command) {
      executor.execute(() -> {
        beforeRun.run();
        try {
          command.run();
        } finally {
          afterRun.run();
        }
      });
    }
  }
}

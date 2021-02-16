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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensures that only a single task (collapse-intention-log) for a specific key (branch-ID) is being
 * active at any time. While a collapse-intention-log for a branch is currently active, another
 * submitted collapse-intention-log task for the same branch will be queued. The last submitted
 * collapse-intention-log task will be run after the currently executing task has finished.
 *
 * <p>This approach reduces contention on the database and also reduces read/write operations.</p>
 *
 * @param <K> Key type
 */
final class BranchCollapseSync<K> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BranchCollapseSync.class);

  private static final String DISABLE_PROPERTY = "nessie.versioned.impl.no-branch-collapse-sync";

  /**
   * Setting the system property {@value #DISABLE_PROPERTY} to {@code true} disables the
   * synchronization.
   */
  private static final boolean DISABLE = Boolean.getBoolean(DISABLE_PROPERTY);

  private final ConcurrentMap<K, BranchCollapseState<K>> collapses = new ConcurrentHashMap<>();

  BranchCollapseSync() {
    // empty
  }

  void submitCollapse(Executor executor, K key, Runnable handler) {
    if (!DISABLE) {
      collapses.compute(key, (k, state) -> {
        if (state != null && state.scheduleOther(handler)) {
          return state;
        }
        return BranchCollapseState.newState(executor, key, handler, collapses::remove);
      });
    } else {
      executor.execute(handler);
    }
  }

  private static final class BranchCollapseState<K> {
    private final K key;
    private final BiConsumer<K, BranchCollapseState<K>> finished;

    private volatile Runnable handler;
    private volatile boolean done;
    private volatile int reSchedules;

    static <K> BranchCollapseState<K> newState(Executor executor, K key, Runnable handler, BiConsumer<K, BranchCollapseState<K>> finished) {
      BranchCollapseState<K> s = new BranchCollapseState<>(key, handler, finished);
      if (executor == null) {
        executor = ForkJoinPool.commonPool();
      }
      executor.execute(s::work);
      return s;
    }

    private BranchCollapseState(K key, Runnable handler, BiConsumer<K, BranchCollapseState<K>> finished) {
      this.key = key;
      this.handler = handler;
      this.finished = finished;
    }

    private void work() {
      int runs = 0;
      while (true) {
        Runnable handler = takeHandler();
        if (handler == null) {
          // Must not call `finished.accept` (which is `collapses.remove(key, this)`) while holding
          // the lock on `this` (`synchronized`), because that would eventually lead to a deadlock
          // between a `ConcurrentHaspMap.Node` and `this`.
          // This delegates to `Map.remove(Object key, Object expectedValue)` to ensure that the
          // map entry does not remove a potentially _newer_ instance of `BranchCollapseState`.
          finished.accept(key, this);
          LOGGER.debug("Exiting work-loop for branch-collapse for ID:{} after {} runs, for {} re-schedules", key, runs, reSchedules);
          break;
        }

        try {
          LOGGER.debug("Running work for branch-collapse for ID:{}", key);
          runs++;
          handler.run();
        } catch (Exception e) {
          LOGGER.warn("Branch-collapse worker run failed", e);
        }
      }
    }

    private synchronized Runnable takeHandler() {
      // Do not access `BranchCollapseSync.collapses` from this synchronized method!
      // (See notes in `work()`.)
      Runnable h = handler;
      handler = null;
      if (h == null) {
        done = true;
      }
      return h;
    }

    synchronized boolean scheduleOther(Runnable handler) {
      // Do not access `BranchCollapseSync.collapses` from this synchronized method!
      // (See notes in `work()`.)
      if (!done) {
        reSchedules++;
        this.handler = handler;
        return true;
      } else {
        return false;
      }
    }
  }
}

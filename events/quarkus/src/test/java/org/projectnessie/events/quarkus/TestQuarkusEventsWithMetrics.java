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
package org.projectnessie.events.quarkus;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.events.quarkus.assertions.EventAssertions;
import org.projectnessie.events.quarkus.assertions.MetricsAssertions;
import org.projectnessie.events.quarkus.assertions.TracingAssertions;
import org.projectnessie.events.quarkus.scenarios.EventScenarios;

@QuarkusTest
@TestProfile(TestQuarkusEventsWithMetrics.Profile.class)
class TestQuarkusEventsWithMetrics {

  @Inject EventScenarios scenarios;
  @Inject EventAssertions events;
  @Inject TracingAssertions tracing;
  @Inject MetricsAssertions metrics;

  @AfterEach
  void reset() {
    events.reset();
    tracing.reset();
    metrics.reset();
  }

  @Test
  public void testCommitWithMetrics() {
    scenarios.commit();
    events.awaitAndAssertCommitEvents(true);
    tracing.assertNoNessieTraces();
    metrics.awaitAndAssertCommitMetrics();
  }

  @Test
  public void testMergeWithMetrics() {
    scenarios.merge();
    events.awaitAndAssertMergeEvents(true);
    tracing.assertNoNessieTraces();
    metrics.awaitAndAssertMergeMetrics();
  }

  @Test
  public void testTransplantWithMetrics() {
    scenarios.transplant();
    events.awaitAndAssertTransplantEvents(true);
    tracing.assertNoNessieTraces();
    metrics.awaitAndAssertTransplantMetrics();
  }

  @Test
  public void testReferenceCreatedWithMetrics() {
    scenarios.referenceCreated();
    events.awaitAndAssertReferenceCreatedEvents(true);
    tracing.assertNoNessieTraces();
    metrics.awaitAndAssertReferenceCreatedMetrics();
  }

  @Test
  public void testReferenceDeletedWithMetrics() {
    scenarios.referenceDeleted();
    events.awaitAndAssertReferenceDeletedEvents(true);
    tracing.assertNoNessieTraces();
    metrics.awaitAndAssertReferenceDeletedMetrics();
  }

  @Test
  public void testReferenceUpdatedWithMetrics() {
    scenarios.referenceUpdated();
    events.awaitAndAssertReferenceUpdatedEvents(true);
    tracing.assertNoNessieTraces();
    metrics.awaitAndAssertReferenceUpdatedMetrics();
  }

  public static class Profile extends TestQuarkusEvents.Profile {

    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> map = new HashMap<>(super.getConfigOverrides());
      map.put("nessie.version.store.events.metrics.enable", "true");
      return map;
    }
  }
}

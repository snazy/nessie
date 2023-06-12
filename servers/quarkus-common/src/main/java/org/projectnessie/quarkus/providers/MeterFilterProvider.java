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
package org.projectnessie.quarkus.providers;

import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

public class MeterFilterProvider {

  public static final String APPLICATION_TAG_NAME = "application";
  public static final String APPLICATION_TAG_VALUE = "Nessie";

  @Produces
  @Singleton
  public MeterFilter produceGlobalMeterFilter() {
    return MeterFilter.commonTags(Tags.of(APPLICATION_TAG_NAME, APPLICATION_TAG_VALUE));
  }
}

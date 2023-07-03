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
package org.projectnessie.catalog.service.services;

import org.immutables.value.Value;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.service.spi.DecodedPrefix;
import org.projectnessie.catalog.service.spi.TenantSpecific;
import org.projectnessie.catalog.service.spi.Warehouse;

@Value.Immutable
public interface ServiceContext {
  @Value.Parameter(order = 1)
  TenantSpecific tenantSpecific();

  @Value.Parameter(order = 2)
  ParsedReference parsedReference();

  @Value.Parameter(order = 3)
  Warehouse warehouse();

  static ServiceContext serviceContext(
      TenantSpecific tenantSpecific, ParsedReference parsedReference, Warehouse warehouse) {
    return ImmutableServiceContext.of(tenantSpecific, parsedReference, warehouse);
  }

  static ServiceContext serviceContext(TenantSpecific tenantSpecific, DecodedPrefix decoded) {
    return serviceContext(
        tenantSpecific,
        decoded.parsedReference(),
        tenantSpecific.getWarehouse(decoded.warehouse()));
  }
}

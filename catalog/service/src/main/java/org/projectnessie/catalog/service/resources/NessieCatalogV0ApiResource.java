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
package org.projectnessie.catalog.service.resources;

import static org.projectnessie.catalog.service.services.ServiceContext.serviceContext;

import java.io.IOException;
import java.util.UUID;
import javax.enterprise.context.RequestScoped;
import org.projectnessie.catalog.api.NessieCatalogV0Api;
import org.projectnessie.catalog.api.model.AddDataFilesOutcome;
import org.projectnessie.catalog.api.model.AddDataFilesRequest;
import org.projectnessie.catalog.api.model.CatalogConfig;
import org.projectnessie.catalog.api.model.ImmutableMergeSession;
import org.projectnessie.catalog.api.model.MergeOutcome;
import org.projectnessie.catalog.api.model.MergeRequest;
import org.projectnessie.catalog.api.model.MergeSession;
import org.projectnessie.catalog.api.model.PickTablesOutcome;
import org.projectnessie.catalog.api.model.PickTablesRequest;
import org.projectnessie.catalog.service.services.MergeService;
import org.projectnessie.catalog.service.spi.DecodedPrefix;
import org.projectnessie.catalog.service.spi.TableRef;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.MergeResponse;

@RequestScoped
@jakarta.enterprise.context.RequestScoped
public class NessieCatalogV0ApiResource extends BaseIcebergResource implements NessieCatalogV0Api {

  @Override
  public CatalogConfig config() {
    return null;
  }

  @Override
  public AddDataFilesOutcome addDataFiles(
      String prefix, String namespace, String table, AddDataFilesRequest addDataFiles)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    return null;
  }

  @Override
  public PickTablesOutcome pickTables(String prefix, PickTablesRequest pickTables)
      throws IOException {
    DecodedPrefix decoded = decodePrefix(prefix);

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    return null;
  }

  @Override
  public MergeOutcome merge(String prefix, MergeRequest merge) throws IOException {
    DecodedPrefix decoded = decodePrefix(prefix);

    MergeSession session = merge.getSession();

    if (session == null) {
      // TODO Implement merge session management.
      // IDs can be random.
      // The signature must be generated using the ID, the user ID, and a secret.
      // Persistent state can be __LATER__ built upon this, to cache previous conflict resolutions,
      // cache content, etc, if necessary.
      String id = UUID.randomUUID().toString();
      String sig = "";
      session = ImmutableMergeSession.of(id, sig);
    }

    MergeService mergeService = new MergeService(serviceContext(tenantSpecific, decoded));

    MergeResponse response = mergeService.merge(merge).getResponse();

    return MergeOutcome.builder().session(session).response(response).build();
  }
}

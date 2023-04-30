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
package org.projectnessie.services.rest;

import io.smallrye.common.annotation.RunOnVirtualThread;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import org.projectnessie.api.v1.http.HttpRefLogApi;
import org.projectnessie.api.v1.params.RefLogParams;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.RefLogResponse;
import org.projectnessie.services.spi.RefLogService;

/** REST endpoint for the reflog-API. */
@RequestScoped
@jakarta.enterprise.context.RequestScoped
@Deprecated
@RunOnVirtualThread
public class RestRefLogResource implements HttpRefLogApi {

  private final RefLogService refLogService;

  // Mandated by CDI 2.0
  public RestRefLogResource() {
    this(null);
  }

  @Inject
  @jakarta.inject.Inject
  public RestRefLogResource(RefLogService refLogService) {
    this.refLogService = refLogService;
  }

  private RefLogService resource() {
    return refLogService;
  }

  @Override
  @RunOnVirtualThread
  public RefLogResponse getRefLog(RefLogParams params) throws NessieNotFoundException {
    return resource()
        .getRefLog(
            params.startHash(),
            params.endHash(),
            params.filter(),
            params.maxRecords(),
            params.pageToken());
  }
}

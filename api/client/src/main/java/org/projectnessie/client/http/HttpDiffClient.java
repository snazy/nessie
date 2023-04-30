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
package org.projectnessie.client.http;

import org.projectnessie.api.v1.http.HttpDiffApi;
import org.projectnessie.api.v1.params.DiffParams;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;

class HttpDiffClient implements HttpDiffApi {

  private final HttpClient client;

  public HttpDiffClient(HttpClient client) {
    this.client = client;
  }

  @Override
  public DiffResponse getDiff(String fromRefWithHash, String toRefWithHash)
      throws NessieNotFoundException {
    DiffParams params =
        DiffParams.builder().fromHashOnRef(fromRefWithHash).toHashOnRef(toRefWithHash).build();
    return client
        .newRequest()
        .path("diffs/{fromRef}{fromHashOnRef}...{toRef}{toHashOnRef}")
        .resolveTemplate("fromRef", params.getFromRef())
        .resolveTemplate("toRef", params.getToRef())
        .resolveTemplate(
            "fromHashOnRef",
            params.getFromHashOnRef() != null ? "*" + params.getFromHashOnRef() : "")
        .resolveTemplate(
            "toHashOnRef", params.getToHashOnRef() != null ? "*" + params.getToHashOnRef() : "")
        .get()
        .readEntity(DiffResponse.class);
  }
}

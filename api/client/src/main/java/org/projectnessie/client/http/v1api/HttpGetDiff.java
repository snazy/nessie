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
package org.projectnessie.client.http.v1api;

import org.projectnessie.api.v1.params.DiffParams;
import org.projectnessie.client.api.GetDiffBuilder;
import org.projectnessie.client.builder.BaseGetDiffBuilder;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;

final class HttpGetDiff extends BaseGetDiffBuilder<DiffParams> {

  private static final String PAGINATION_ERROR_MESSAGE =
      "Diff pagination is not supported in API v1.";

  private final NessieApiClient client;

  HttpGetDiff(NessieApiClient client) {
    // Note: the token is ignored, its is not supported by Diff API v1
    super((params, pageToken) -> params);
    this.client = client;
  }

  @Override
  public GetDiffBuilder pageToken(String pageToken) {
    throw new UnsupportedOperationException(PAGINATION_ERROR_MESSAGE);
  }

  @Override
  public GetDiffBuilder maxRecords(int maxRecords) {
    throw new UnsupportedOperationException(PAGINATION_ERROR_MESSAGE);
  }

  @Override
  protected DiffParams params() {
    return DiffParams.builder()
        .fromRef(fromRefName)
        .fromHashOnRef(fromHashOnRef)
        .toRef(toRefName)
        .toHashOnRef(toHashOnRef)
        .build();
  }

  @Override
  public DiffResponse get(DiffParams params) throws NessieNotFoundException {
    return client.getDiffApi().getDiff(params.getFromPathParam(), params.getToPathParam());
  }
}

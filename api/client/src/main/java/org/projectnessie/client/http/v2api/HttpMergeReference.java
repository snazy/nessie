/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.client.http.v2api;

import org.projectnessie.api.v2.params.ImmutableMerge;
import org.projectnessie.api.v2.params.Merge;
import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.client.api.MergeResponseInspector;
import org.projectnessie.client.api.impl.MergeResponseInspectorImpl;
import org.projectnessie.client.builder.BaseMergeReferenceBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Reference;

final class HttpMergeReference extends BaseMergeReferenceBuilder {
  private final HttpApiV2 api;
  private final HttpClient client;

  public HttpMergeReference(HttpApiV2 api, HttpClient client) {
    this.api = api;
    this.client = client;
  }

  @Override
  public MergeReferenceBuilder keepIndividualCommits(boolean keepIndividualCommits) {
    if (keepIndividualCommits) {
      throw new IllegalArgumentException("Commits are always squashed during merge operations.");
    }
    return this;
  }

  private Merge buildMerge() {
    ImmutableMerge.Builder merge =
        ImmutableMerge.builder()
            .fromHash(fromHash)
            .fromRefName(fromRefName)
            .message(message)
            .commitMeta(commitMeta)
            .isDryRun(dryRun)
            .isFetchAdditionalInfo(fetchAdditionalInfo)
            .isReturnConflictAsResult(returnConflictAsResult);

    if (defaultMergeMode != null) {
      merge.defaultKeyMergeMode(defaultMergeMode);
    }

    if (mergeModes != null) {
      merge.keyMergeModes(mergeModes.values());
    }

    return merge.build();
  }

  private MergeResponse submitMergeRequest(Merge request)
      throws NessieNotFoundException, NessieConflictException {
    return client
        .newRequest()
        .path("trees/{ref}/history/merge")
        .resolveTemplate("ref", Reference.toPathString(branchName, hash))
        .unwrap(NessieNotFoundException.class, NessieConflictException.class)
        .post(request)
        .readEntity(MergeResponse.class);
  }

  @Override
  public MergeResponse merge() throws NessieNotFoundException, NessieConflictException {
    Merge request = buildMerge();

    return submitMergeRequest(request);
  }

  @Override
  public MergeResponseInspector mergeInspect()
      throws NessieNotFoundException, NessieConflictException {
    Merge request = buildMerge();

    MergeResponse response = submitMergeRequest(request);

    return MergeResponseInspectorImpl.builder()
        .api(api)
        .request(request)
        .response(response)
        .build();
  }
}

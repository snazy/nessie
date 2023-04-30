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

import static org.projectnessie.services.impl.RefUtil.toReference;

import com.fasterxml.jackson.annotation.JsonView;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import org.projectnessie.api.v1.http.HttpDiffApi;
import org.projectnessie.api.v1.params.DiffParams;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.ImmutableDiffResponse;
import org.projectnessie.model.ser.Views;
import org.projectnessie.services.spi.DiffService;
import org.projectnessie.services.spi.PagedResponseHandler;

/** REST endpoint for the diff-API. */
@RequestScoped
@jakarta.enterprise.context.RequestScoped
public class RestDiffResource implements HttpDiffApi {
  // Cannot extend the DiffApiImplWithAuthz class, because then CDI gets confused
  // about which interface to use - either HttpContentApi or the plain ContentApi. This can lead
  // to various symptoms: complaints about varying validation-constraints in HttpTreeApi + TreeAPi,
  // empty resources (no REST methods defined) and potentially other.

  private final DiffService diffService;

  // Mandated by CDI 2.0
  public RestDiffResource() {
    this(null);
  }

  @Inject
  @jakarta.inject.Inject
  public RestDiffResource(DiffService diffService) {
    this.diffService = diffService;
  }

  private DiffService resource() {
    return diffService;
  }

  @Override
  @JsonView(Views.V1.class)
  public DiffResponse getDiff(String fromRefWithHash, String toRefWithHash)
      throws NessieNotFoundException {
    DiffParams params =
        DiffParams.builder().fromHashOnRef(fromRefWithHash).toHashOnRef(toRefWithHash).build();
    ImmutableDiffResponse.Builder builder = DiffResponse.builder();
    return resource()
        .getDiff(
            params.getFromRef(),
            params.getFromHashOnRef(),
            params.getToRef(),
            params.getToHashOnRef(),
            null,
            new PagedResponseHandler<DiffResponse, DiffEntry>() {

              @Override
              public DiffResponse build() {
                return builder.build();
              }

              @Override
              public boolean addEntry(DiffEntry entry) {
                builder.addDiffs(entry);
                return true;
              }

              @Override
              public void hasMore(String pagingToken) {
                builder.isHasMore(true).token(pagingToken);
              }
            },
            h -> builder.effectiveFromReference(toReference(h)),
            h -> builder.effectiveToReference(toReference(h)));
  }
}

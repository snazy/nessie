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

import com.fasterxml.jackson.annotation.JsonView;
import io.smallrye.common.annotation.RunOnVirtualThread;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import org.projectnessie.api.v1.http.HttpContentApi;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.ser.Views;
import org.projectnessie.services.spi.ContentService;

/** REST endpoint for the content-API. */
@RequestScoped
@jakarta.enterprise.context.RequestScoped
@RunOnVirtualThread
public class RestContentResource implements HttpContentApi {
  // Cannot extend the ContentApiImplWithAuthz class, because then CDI gets confused
  // about which interface to use - either HttpContentApi or the plain ContentApi. This can lead
  // to various symptoms: complaints about varying validation-constraints in HttpTreeApi + TreeAPi,
  // empty resources (no REST methods defined) and potentially other.

  private final ContentService contentService;

  // Mandated by CDI 2.0
  public RestContentResource() {
    this(null);
  }

  @Inject
  @jakarta.inject.Inject
  public RestContentResource(ContentService contentService) {
    this.contentService = contentService;
  }

  private ContentService resource() {
    return contentService;
  }

  @Override
  @JsonView(Views.V1.class)
  public Content getContent(ContentKey key, String ref, String hashOnRef)
      throws NessieNotFoundException {
    return resource().getContent(key, ref, hashOnRef).getContent();
  }

  @Override
  @JsonView(Views.V1.class)
  public GetMultipleContentsResponse getMultipleContents(
      String ref, String hashOnRef, GetMultipleContentsRequest request)
      throws NessieNotFoundException {
    return resource().getMultipleContents(ref, hashOnRef, request.getRequestedKeys());
  }
}

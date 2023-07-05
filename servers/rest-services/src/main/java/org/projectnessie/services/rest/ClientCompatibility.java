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
package org.projectnessie.services.rest;

import java.util.List;
import java.util.stream.Collectors;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ImmutableMergeResponse;
import org.projectnessie.model.MergeResponse;

public final class ClientCompatibility {

  private ClientCompatibility() {}

  @FunctionalInterface
  interface Committing<R> {
    R run() throws NessieNotFoundException, NessieConflictException;
  }

  @SuppressWarnings("unchecked")
  static <R> R maybeUpdateResponseOrException(String nessieClientSpec, Committing<R> committing)
      throws NessieNotFoundException, NessieConflictException {
    if (isNessieClientSpec(nessieClientSpec, 3)) {
      return committing.run();
    }

    // Strip 'renameTo' field from 'Conflict' objects...
    try {
      R response = committing.run();
      if (response instanceof MergeResponse) {
        response = (R) mergeResponseWithoutRenameTo((MergeResponse) response);
      }
      return response;
    } catch (NessieReferenceConflictException e) {
      throw new NessieReferenceConflictException(
          referenceConflictsWithoutRenameTo(e.getErrorDetails()), e.getMessage(), e);
    }
  }

  static ReferenceConflicts referenceConflictsWithoutRenameTo(
      ReferenceConflicts referenceConflicts) {
    return ReferenceConflicts.referenceConflicts(
        conflictsWithoutRenameTo(referenceConflicts.conflicts()));
  }

  static MergeResponse mergeResponseWithoutRenameTo(MergeResponse mergeResponse) {
    return ImmutableMergeResponse.builder()
        .from(mergeResponse)
        .details(contentKeyDetailsWithoutRenameTo(mergeResponse.getDetails()))
        .build();
  }

  private static List<MergeResponse.ContentKeyDetails> contentKeyDetailsWithoutRenameTo(
      List<MergeResponse.ContentKeyDetails> details) {
    return details.stream()
        .map(ClientCompatibility::contentKeyDetailWithoutRenameTo)
        .collect(Collectors.toList());
  }

  private static MergeResponse.ContentKeyDetails contentKeyDetailWithoutRenameTo(
      MergeResponse.ContentKeyDetails contentKeyDetails) {
    return contentKeyDetails.withConflict(conflictWithoutRenameTo(contentKeyDetails.getConflict()));
  }

  static List<Conflict> conflictsWithoutRenameTo(List<Conflict> conflicts) {
    return conflicts.stream()
        .map(ClientCompatibility::conflictWithoutRenameTo)
        .collect(Collectors.toList());
  }

  static Conflict conflictWithoutRenameTo(Conflict conflict) {
    return conflict == null ? null : conflict.withRenameTo(null).withContentId(null);
  }

  static boolean isNessieClientSpec(String nessieClientSpec, int spec) {
    int clientSpec = 0;
    if (nessieClientSpec != null) {
      try {
        clientSpec = Integer.parseInt(nessieClientSpec.trim());
      } catch (Exception ignore) {
      }
    }
    return clientSpec >= spec;
  }
}

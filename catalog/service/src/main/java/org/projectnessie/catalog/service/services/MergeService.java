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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.api.model.MergeRequest;
import org.projectnessie.catalog.content.merge.ContentAwareMerge;
import org.projectnessie.catalog.content.merge.ContentMerge;
import org.projectnessie.catalog.content.merge.ImmutableContentMerge;
import org.projectnessie.catalog.service.spi.Warehouse;
import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.client.api.MergeResponseInspector;
import org.projectnessie.client.api.MergeResponseInspector.MergeConflictDetails;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.MergeResponse;

public class MergeService extends BaseService {
  public MergeService(ServiceContext context) {
    super(context);
  }

  public MergeResponseInspector merge(MergeRequest merge) throws IOException {
    @SuppressWarnings("resource")
    NessieApiV2 api = context().tenantSpecific().api();

    ParsedReference parsedReference = context().parsedReference();
    String targetBranch = parsedReference.name();
    String expectedHash = parsedReference.hashWithRelativeSpec();
    checkArgument(targetBranch != null, "No target branch specified.");
    checkArgument(expectedHash != null, "No expected hash specified.");

    @SuppressWarnings("DataFlowIssue")
    boolean dryRun = merge.isDryRun() != null ? merge.isDryRun() : false;

    // TODO handle the case when the target branch "moves ahead" - have to clear all resolved
    //  contents

    Map<ContentKey, Content> resolvedContents = new HashMap<>();
    try {
      while (true) {
        MergeReferenceBuilder mergeReference =
            api.mergeRefIntoBranch()
                .branchName(targetBranch)
                .hash(expectedHash)
                .returnConflictAsResult(true)
                .defaultMergeMode(merge.getDefaultKeyMergeMode())
                .fromHash(merge.getFromHash())
                .fromRefName(merge.getFromRefName())
                .dryRun(dryRun)
                .commitMeta(merge.getCommitMeta());

        for (Map.Entry<ContentKey, Content> resolved : resolvedContents.entrySet()) {
          mergeReference.mergeKeyBehavior(
              MergeKeyBehavior.of(
                  resolved.getKey(), MergeBehavior.NORMAL, null, resolved.getValue()));
        }

        MergeResponseInspector responseInspector =
            mergeReference
                // TODO .mergeMode()
                .mergeInspect();

        MergeResponse response = responseInspector.getResponse();

        if (response.wasSuccessful()) {
          return responseInspector;
        }

        boolean unresolvable = false;
        List<ContentMerge<?>> contentMerges = new ArrayList<>();
        for (Iterator<MergeConflictDetails> details =
                responseInspector.collectMergeConflictDetails().iterator();
            details.hasNext(); ) {
          MergeConflictDetails detail = details.next();

          Conflict conflict = detail.conflict();

          if (conflict != null) {
            Conflict.ConflictType conflictType = conflict.conflictType();
            if (conflictType != null) {
              switch (conflictType) {
                case VALUE_DIFFERS:
                  ContentMerge<?> contentMerge = validateValueDiffers(detail);
                  if (contentMerge != null) {
                    contentMerges.add(contentMerge);
                  } else {
                    unresolvable = true;
                  }
                  break;
                default:
                  unresolvable = true;
                  break;
              }
            }
          }
        }

        if (unresolvable) {
          // There is at least one conflict that cannot be resolved.
          return responseInspector;
        }

        // At this point we have only potentially resolvable conflicts. This means, that all
        // conflicts are known to:
        // - affect only Iceberg tables
        // - have differing values
        // Examples(!) of conflicts that prevents the code to get here:
        // - another content type than ICEBERG_TABLE
        // - creation of the same Iceberg table using the same keys on both source + target
        // - Iceberg table modified on source, but dropped on target
        // - etc.

        // TODO use the resolvedTable using the RIGHT content-key (valid for on merge-base) as the
        //  explicitly resolved content in the next merge-attempt.
        // TODO we could potentially remember a merge-resolution based on the merge-base, source and
        //  target contents (three values!). This should make retries much quicker. If the resulting
        //  Iceberg table-metadata is persisted, it can be reused. This would be Nessie's "rerere
        //  plugin". An Iceberg content-aware merge can be an expensive operation, it definitely
        //  requires reading table-metadata, manifest-lists and manifest files.
        for (ContentMerge<?> contentMerge : contentMerges) {
          performContentMerge(contentMerge)
              .ifPresent(
                  resolvedTable -> resolvedContents.put(contentMerge.sourceKey(), resolvedTable));
        }
      }
    } catch (NessieConflictException conflict) {
      throw new RuntimeException(conflict);
    }
  }

  private ContentMerge<?> validateValueDiffers(MergeConflictDetails detail) {
    ContentKey key = detail.keyOnMergeBase();
    Content mergeBaseContent = detail.contentOnMergeBase();
    Content.Type contentType = mergeBaseContent.getType();

    if (contentType != Content.Type.ICEBERG_TABLE) {
      return null;
    }

    String contentId = mergeBaseContent.getId();

    Content sourceContent = detail.contentOnSource();
    Content targetContent = detail.contentOnTarget();

    checkState(
        contentId != null,
        "Expect merge-base to have a content ID, but key %s has none",
        contentId,
        key);
    checkState(
        sourceContent != null,
        "Expect diff on source for content ID %s / key %s, but found none",
        contentId,
        key);
    checkState(
        targetContent != null,
        "Expect diff on target for content ID %s / key %s, but found none",
        contentId,
        key);
    checkState(
        contentId.equals(sourceContent.getId()),
        "Expect same content on source for content ID %s / key %s, but found %s",
        contentId,
        key,
        sourceContent.getId());
    checkState(
        contentId.equals(targetContent.getId()),
        "Expect same content on target for content ID %s / key %s, but found %s",
        contentId,
        key,
        targetContent.getId());

    // At this point:
    // - 'sourceContent' contains the content on the merge-source, potentially renamed on the source
    //   since the merge-base
    // - 'targetContent' contains the content on the merge-target, potentially renamed on the target
    //   since the merge-base

    Warehouse warehouse = context().warehouse();

    return ImmutableContentMerge.builder()
        .sourceKey(key)
        .contentType(contentType)
        .mergeBaseTable(mergeBaseContent)
        .sourceTable(sourceContent)
        .targetTable(targetContent)
        .fileIO(warehouse.fileIO())
        .metadataIO(warehouse.metadataIO())
        .build();
  }

  private Optional<? extends Content> performContentMerge(
      ContentMerge<? extends Content> contentMerge) throws IOException {
    return ContentAwareMerge.mergeContent(contentMerge);
  }
}

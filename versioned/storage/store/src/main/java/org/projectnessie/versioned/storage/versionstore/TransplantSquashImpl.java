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
package org.projectnessie.versioned.storage.versionstore;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableMergeResult;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MergeType;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.storage.common.logic.CommitRetry.RetryException;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

final class TransplantSquashImpl extends BaseMergeTransplantSquash implements Transplant {

  TransplantSquashImpl(
      @Nonnull @jakarta.annotation.Nonnull BranchName branch,
      @Nonnull @jakarta.annotation.Nonnull Optional<Hash> referenceHash,
      @Nonnull @jakarta.annotation.Nonnull Persist persist,
      @Nonnull @jakarta.annotation.Nonnull Reference reference,
      @Nullable @jakarta.annotation.Nullable CommitObj head)
      throws ReferenceNotFoundException {
    super(branch, referenceHash, persist, reference, head);
  }

  @Override
  public MergeResult<Commit> transplant(
      Optional<?> retryState,
      List<Hash> sequenceToTransplant,
      MetadataRewriter<CommitMeta> updateCommitMetadata,
      Map<ContentKey, MergeType> mergeTypes,
      MergeType defaultMergeType,
      boolean dryRun)
      throws ReferenceNotFoundException, RetryException {
    SourceCommitsAndParent sourceCommits = loadSourceCommitsPlusParent(sequenceToTransplant);

    ImmutableMergeResult.Builder<Commit> mergeResult = prepareMergeResult();

    Function<ContentKey, MergeType> mergeTypeForKey =
        key -> mergeTypes.getOrDefault(key, defaultMergeType);

    return squash(dryRun, mergeResult, mergeTypeForKey, updateCommitMetadata, sourceCommits, null);
  }
}

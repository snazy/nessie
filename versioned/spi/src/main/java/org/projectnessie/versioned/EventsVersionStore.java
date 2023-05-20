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
package org.projectnessie.versioned;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.versioned.paging.PaginationIterator;

/**
 * A {@link VersionStore} wrapper that publishes results when a method is called that changes the
 * catalog state.
 */
public class EventsVersionStore implements VersionStore {

  private final VersionStore delegate;
  private final Consumer<Result> resultSink;

  /**
   * Takes the {@link VersionStore} instance to enrich with events.
   *
   * @param delegate backing/delegate {@link VersionStore}.
   * @param resultSink a consumer for results.
   */
  public EventsVersionStore(VersionStore delegate, Consumer<Result> resultSink) {
    this.delegate = delegate;
    this.resultSink = resultSink;
  }

  @Override
  public CommitResult<Commit> commit(
      @Nonnull @jakarta.annotation.Nonnull BranchName branch,
      @Nonnull @jakarta.annotation.Nonnull Optional<Hash> referenceHash,
      @Nonnull @jakarta.annotation.Nonnull CommitMeta metadata,
      @Nonnull @jakarta.annotation.Nonnull List<Operation> operations,
      @Nonnull @jakarta.annotation.Nonnull Callable<Void> validator,
      @Nonnull @jakarta.annotation.Nonnull BiConsumer<ContentKey, String> addedContents)
      throws ReferenceNotFoundException, ReferenceConflictException {
    CommitResult<Commit> result =
        delegate.commit(branch, referenceHash, metadata, operations, validator, addedContents);
    resultSink.accept(result);
    return result;
  }

  @Override
  public MergeResult<Commit> transplant(
      NamedRef sourceRef,
      BranchName targetBranch,
      Optional<Hash> referenceHash,
      List<Hash> sequenceToTransplant,
      MetadataRewriter<CommitMeta> updateCommitMetadata,
      boolean keepIndividualCommits,
      Map<ContentKey, MergeKeyBehavior> mergeKeyBehaviors,
      MergeBehavior defaultMergeBehavior,
      boolean dryRun,
      boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    MergeResult<Commit> result =
        delegate.transplant(
            sourceRef,
            targetBranch,
            referenceHash,
            sequenceToTransplant,
            updateCommitMetadata,
            keepIndividualCommits,
            mergeKeyBehaviors,
            defaultMergeBehavior,
            dryRun,
            fetchAdditionalInfo);
    if (result.wasApplied()) {
      resultSink.accept(result);
    }
    return result;
  }

  @Override
  public MergeResult<Commit> merge(
      NamedRef fromRef,
      Hash fromHash,
      BranchName toBranch,
      Optional<Hash> expectedHash,
      MetadataRewriter<CommitMeta> updateCommitMetadata,
      boolean keepIndividualCommits,
      Map<ContentKey, MergeKeyBehavior> mergeKeyBehaviors,
      MergeBehavior defaultMergeBehavior,
      boolean dryRun,
      boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    MergeResult<Commit> result =
        delegate.merge(
            fromRef,
            fromHash,
            toBranch,
            expectedHash,
            updateCommitMetadata,
            keepIndividualCommits,
            mergeKeyBehaviors,
            defaultMergeBehavior,
            dryRun,
            fetchAdditionalInfo);
    if (result.wasApplied()) {
      resultSink.accept(result);
    }
    return result;
  }

  @Override
  public ReferenceAssignedResult assign(NamedRef ref, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    ReferenceAssignedResult result = delegate.assign(ref, expectedHash, targetHash);
    resultSink.accept(result);
    return result;
  }

  @Override
  public ReferenceCreatedResult create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    // FIXME half-created refs with new storage model
    ReferenceCreatedResult result = delegate.create(ref, targetHash);
    resultSink.accept(result);
    return result;
  }

  @Override
  public ReferenceDeletedResult delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    ReferenceDeletedResult result = delegate.delete(ref, hash);
    resultSink.accept(result);
    return result;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public RepositoryInformation getRepositoryInformation() {
    return delegate.getRepositoryInformation();
  }

  @Override
  public Hash hashOnReference(NamedRef namedReference, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException {
    return delegate.hashOnReference(namedReference, hashOnReference);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Hash noAncestorHash() {
    return delegate.noAncestorHash();
  }

  @Override
  public ReferenceInfo<CommitMeta> getNamedRef(String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    return delegate.getNamedRef(ref, params);
  }

  @Override
  public PaginationIterator<ReferenceInfo<CommitMeta>> getNamedRefs(
      GetNamedRefsParams params, String pagingToken) throws ReferenceNotFoundException {
    return delegate.getNamedRefs(params, pagingToken);
  }

  @Override
  public PaginationIterator<Commit> getCommits(Ref ref, boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    return delegate.getCommits(ref, fetchAdditionalInfo);
  }

  @Override
  public PaginationIterator<KeyEntry> getKeys(
      Ref ref,
      String pagingToken,
      boolean withContent,
      ContentKey minKey,
      ContentKey maxKey,
      ContentKey prefixKey,
      Predicate<ContentKey> contentKeyPredicate)
      throws ReferenceNotFoundException {
    return delegate.getKeys(
        ref, pagingToken, withContent, minKey, maxKey, prefixKey, contentKeyPredicate);
  }

  @Override
  public Content getValue(Ref ref, ContentKey key) throws ReferenceNotFoundException {
    return delegate.getValue(ref, key);
  }

  @Override
  public Map<ContentKey, Content> getValues(Ref ref, Collection<ContentKey> keys)
      throws ReferenceNotFoundException {
    return delegate.getValues(ref, keys);
  }

  @Override
  public PaginationIterator<Diff> getDiffs(
      Ref from,
      Ref to,
      String pagingToken,
      ContentKey minKey,
      ContentKey maxKey,
      ContentKey prefixKey,
      Predicate<ContentKey> contentKeyPredicate)
      throws ReferenceNotFoundException {
    return delegate.getDiffs(from, to, pagingToken, minKey, maxKey, prefixKey, contentKeyPredicate);
  }

  @Override
  @Deprecated
  @SuppressWarnings("MustBeClosedChecker")
  public Stream<RefLogDetails> getRefLog(Hash refLogId) throws RefLogNotFoundException {
    return delegate.getRefLog(refLogId);
  }
}
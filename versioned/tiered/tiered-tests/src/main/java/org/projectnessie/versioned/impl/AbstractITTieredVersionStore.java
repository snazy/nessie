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
package org.projectnessie.versioned.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.SerializerWithPayload;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.WithType;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tests.AbstractITVersionStore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Contains classes that are specific to {@link TieredVersionStore} implementations and therefore
 * do not fit into {@link AbstractITVersionStore}.
 */
public abstract class AbstractITTieredVersionStore extends AbstractITVersionStore {

  private AbstractTieredStoreFixture<?, ?> fixture;

  @BeforeEach
  void setup() {
    fixture = createNewFixture();
  }

  @AfterEach
  void deleteResources() throws Exception {
    fixture.close();
  }

  public VersionStore<String, String, StringSerializer.TestEnum> versionStore() {
    return fixture;
  }

  public Store store() {
    return fixture.getStore();
  }

  protected abstract AbstractTieredStoreFixture<?, ?> createNewFixture();

  @Disabled("TieredVersionStore doesn't check whether expected-hash is in the target branch")
  @Override
  public void commitWithInvalidReference() throws ReferenceNotFoundException,
      ReferenceConflictException, ReferenceAlreadyExistsException {
    super.commitWithInvalidReference();
  }

  @Nested
  @DisplayName("when transplanting")
  public class WhenTransplanting extends AbstractITVersionStore.WhenTransplanting {
    @Disabled("TieredVersionStore doesn't check whether expected-hash is in the target branch")
    @Override
    protected void checkInvalidBranchHash() throws VersionStoreException {
      super.checkInvalidBranchHash();
    }
  }

  @Test
  void tieredCheckKeyList() throws Exception {
    BranchName branch = BranchName.of("my-key-list");
    versionStore().create(branch, Optional.empty());
    assertEquals(0, EntityType.L2.loadSingle(store(), InternalL2.EMPTY_ID).size());
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world"),
        Put.of(Key.of("no"), "world"),
        Put.of(Key.of("mad mad"), "world")));
    assertEquals(0, EntityType.L2.loadSingle(store(), InternalL2.EMPTY_ID).size());
    assertThat(versionStore().getKeys(branch).map(WithType::getValue).map(Key::toString).collect(ImmutableSet.toImmutableSet()),
        Matchers.containsInAnyOrder("hi", "no", "mad mad"));
  }

  @Test
  void tieredCreateAndDeleteTag() throws Exception {
    TagName tag = TagName.of("foo");

    // check that we can't assign an empty tag.
    assertThrows(IllegalArgumentException.class, () -> versionStore().create(tag,  Optional.empty()));

    // create a tag using the default empty hash.
    versionStore().create(tag, Optional.of(InternalL1.EMPTY_ID.toHash()));
    assertEquals(InternalL1.EMPTY_ID.toHash(), versionStore().toHash(tag));

    // delete without condition
    versionStore().delete(tag, Optional.empty());

    // create a tag using the default empty hash.
    versionStore().create(tag, Optional.of(InternalL1.EMPTY_ID.toHash()));

    createAndDeleteRef(tag);
  }

  @Test
  void tieredCreateAndDeleteBranch() throws Exception {
    BranchName branch = BranchName.of("foo");

    // create a tag using the default empty hash.
    versionStore().create(branch, Optional.of(InternalL1.EMPTY_ID.toHash()));
    assertEquals(InternalL1.EMPTY_ID.toHash(), versionStore().toHash(branch));

    // delete without condition
    versionStore().delete(branch, Optional.empty());

    // create a branch using no commit.
    versionStore().create(branch, Optional.empty());

    // avoid dupe
    assertThrows(ReferenceAlreadyExistsException.class, () -> versionStore().create(branch, Optional.empty()));

    createAndDeleteRef(branch);

    versionStore().create(branch, Optional.empty());
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "world")));
    // check that wrong id is rejected for deletion (valid but not matching)
    assertThrows(ReferenceConflictException.class, () -> versionStore().delete(branch, Optional.of(InternalL1.EMPTY_ID.toHash())));

    // can't use tag delete on branch.
    assertThrows(ReferenceConflictException.class, () -> versionStore().delete(TagName.of("foo"), Optional.empty()));
  }

  private void createAndDeleteRef(NamedRef ref) throws ReferenceNotFoundException, ReferenceConflictException {
    // avoid dupe
    assertThrows(ReferenceAlreadyExistsException.class, () -> versionStore().create(ref, Optional.of(InternalL1.EMPTY_ID.toHash())));

    // check that wrong id is rejected for deletion (non-existing)
    assertThrows(ReferenceConflictException.class, () -> versionStore().delete(ref, Optional.of(Id.EMPTY.toHash())));

    // delete with correct id.
    versionStore().delete(ref, Optional.of(InternalL1.EMPTY_ID.toHash()));
    // avoid create to invalid l1
    assertThrows(ReferenceNotFoundException.class, () -> versionStore().create(
        ref, Optional.of(Id.generateRandom().toHash())));

    // fail on delete of non-existent.
    assertThrows(ReferenceNotFoundException.class, () -> versionStore().delete(ref, Optional.empty()));
  }

  @Test
  void checkpointWithUnsavedL1() throws Exception {
    // KeyList.IncrementalList.generateNewCheckpoint collects parent L1s for a branch
    // via HistoryRetriever to "checkpoint" the keylist. However, this only works if
    // HistoryRetriever has access to both saved AND unsaved L1s, so L1s that are persisted
    // and those that are still in the branch's REF. This test verifies that generateNewCheckpoint
    // does not fail in that case.

    BranchName branch = BranchName.of("checkpointWithUnsavedL1");

    versionStore().create(branch, Optional.empty());

    InternalRefId ref = InternalRefId.of(branch);

    // generate MAX_DELTAS-1 keys in the key-list - just enough to *NOT*
    // trigger KeyList.IncrementalList.generateNewCheckpoint
    for (int i = 1; i < KeyList.IncrementalList.MAX_DELTAS; i++) {
      InternalBranch internalBranch = simulateCommit(ref, i);

      // verify that the branch has an unsaved L1
      assertEquals(i, internalBranch.getCommits().stream().filter(c -> !c.isSaved()).count());
      KeyList keyList = internalBranch.getUpdateState(store()).unsafeGetL1().getKeyList();
      assertFalse(keyList.isFull());
      assertFalse(keyList.isEmptyIncremental());
      KeyList.IncrementalList incrementalList = (KeyList.IncrementalList) keyList;
      assertEquals(i, incrementalList.getDistanceFromCheckpointCommits());
    }

    InternalBranch internalBranch = simulateCommit(ref, KeyList.IncrementalList.MAX_DELTAS);
    KeyList keyList = internalBranch.getUpdateState(store()).unsafeGetL1().getKeyList();
    assertTrue(keyList.isFull());
    assertFalse(keyList.isEmptyIncremental());

  }

  /**
   * This is a copy of {@link TieredVersionStore#commit(BranchName, Optional, Object, List)} that
   * allows the test {@link #checkpointWithUnsavedL1()} to produce commits to a branch with
   * unsaved commits and without collapsing the intention log.
   * <p>It is not particularly great to have a "stripped down" and "heavily adjusted" version
   * of the original {@link TieredVersionStore#commit(BranchName, Optional, Object, List)} in
   * a unit test, but the other option to prepare the pre-requisites for
   * {@link #checkpointWithUnsavedL1()}, namely unsaved commits + uncollapsed branch, would have
   * been to refactor the original method and add a bunch of hooks, which felt too heavy.</p>
   *
   * @param ref branch ID
   * @param num number of the commit
   * @return the updated branch
   */
  private InternalBranch simulateCommit(InternalRefId ref, int num) {
    List<Operation<String>> ops = Collections.singletonList(Put.of(Key.of("key" + num), "foo" + num));
    List<InternalKey> keys = ops.stream().map(op -> new InternalKey(op.getKey())).collect(Collectors.toList());

    SerializerWithPayload<String, StringSerializer.TestEnum> serializer = AbstractTieredStoreFixture.WORKER.getValueSerializer();
    Serializer<String> metadataSerializer = AbstractTieredStoreFixture.WORKER.getMetadataSerializer();

    PartialTree<String, StringSerializer.TestEnum> current = PartialTree.of(serializer, ref, keys);

    String incomingCommit = "metadata";
    InternalCommitMetadata metadata = InternalCommitMetadata.of(metadataSerializer.toBytes(incomingCommit));

    store().load(current.getLoadChain(b -> {
      InternalBranch.UpdateState updateState = b.getUpdateState(store());
      return updateState.unsafeGetL1();
    }, PartialTree.LoadType.NO_VALUES));

    // do updates.
    ops.forEach(op ->
        current.setValueForKey(new InternalKey(op.getKey()), Optional.of(((Put<String>) op).getValue())));

    // save all but l1 and branch.
    store().save(
        Stream.concat(
            current.getMostSaveOps(),
            Stream.of(EntityType.COMMIT_METADATA.createSaveOpForEntity(metadata))
        ).collect(Collectors.toList()));

    PartialTree.CommitOp commitOp = current.getCommitOp(
        metadata.getId(),
        Collections.emptyList(),
        true,
        true);

    InternalRef.Builder<?> builder = EntityType.REF.newEntityProducer();
    boolean updated = store().update(ValueType.REF, ref.getId(),
        commitOp.getUpdateWithCommit(), Optional.of(commitOp.getTreeCondition()), Optional.of(builder));
    assertTrue(updated);
    return builder.build().getBranch();
  }

  @Test
  void checkRefs() throws Exception {
    versionStore().create(BranchName.of("b1"), Optional.empty());
    versionStore().create(BranchName.of("b2"), Optional.empty());
    versionStore().create(TagName.of("t1"), Optional.of(InternalL1.EMPTY_ID.toHash()));
    versionStore().create(TagName.of("t2"), Optional.of(InternalL1.EMPTY_ID.toHash()));
    try (Stream<WithHash<NamedRef>> str = versionStore().getNamedRefs()) {
      assertEquals(ImmutableSet.of("b1", "b2", "t1", "t2"), str
          .map(wh -> wh.getValue().getName()).collect(Collectors.toSet()));
    }
  }

  @Test
  void commitRetryCountExceeded() throws Exception {
    BranchName branch = BranchName.of("commitRetryCountExceeded");
    versionStore().create(branch, Optional.empty());
    String c1 = "c1";
    String c2 = "c2";
    Key k1 = Key.of("hi");
    String v1 = "hello world";
    Key k2 = Key.of("my", "friend");
    String v2 = "not here";

    doReturn(false).when(store()).update(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

    assertEquals("Unable to complete commit due to conflicting events. Retried 5 times before failing.",
        assertThrows(ReferenceConflictException.class,
            () -> versionStore().commit(branch, Optional.empty(), c1, ImmutableList.of(Put.of(k1, v1), Put.of(k2, v2)))).getMessage());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 4})
  void checkCommits(int numStoreUpdateFailures) throws Exception {
    BranchName branch = BranchName.of("checkCommits" + numStoreUpdateFailures);
    versionStore().create(branch, Optional.empty());
    String c1 = "c1";
    String c2 = "c2";
    Key k1 = Key.of("hi");
    String v1 = "hello world";
    String v1p = "goodbye world";
    Key k2 = Key.of("my", "friend");
    String v2 = "not here";

    AtomicInteger commitUpdateTry = new AtomicInteger();
    doAnswer(invocationOnMock -> {
      if (commitUpdateTry.getAndIncrement() < numStoreUpdateFailures) {
        return false;
      }
      return invocationOnMock.callRealMethod();
    }).when(store()).update(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    versionStore().commit(branch, Optional.empty(), c1, ImmutableList.of(Put.of(k1, v1), Put.of(k2, v2)));
    commitUpdateTry.set(0);
    versionStore().commit(branch, Optional.empty(), c2, ImmutableList.of(Put.of(k1, v1p)));

    List<WithHash<String>> commits = versionStore().getCommits(branch).collect(Collectors.toList());
    assertEquals(ImmutableList.of(c2, c1), commits.stream().map(WithHash::getValue).collect(Collectors.toList()));

    // changed across commits
    assertEquals(v1, versionStore().getValue(commits.get(1).getHash(), k1));
    assertEquals(v1p, versionStore().getValue(commits.get(0).getHash(), k1));

    // not changed across commits
    assertEquals(v2, versionStore().getValue(commits.get(0).getHash(), k2));
    assertEquals(v2, versionStore().getValue(commits.get(1).getHash(), k2));

    assertEquals(2, versionStore().getCommits(commits.get(0).getHash()).count());

    TagName tag = TagName.of("tag1");
    versionStore().create(tag, Optional.of(commits.get(0).getHash()));
    assertEquals(2, versionStore().getCommits(tag).count());
  }
}

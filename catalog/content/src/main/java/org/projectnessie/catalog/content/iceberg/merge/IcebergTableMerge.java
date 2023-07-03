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
package org.projectnessie.catalog.content.iceberg.merge;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.immutables.value.Value;
import org.projectnessie.catalog.content.iceberg.ops.CatalogTableOperations;

@Value.Immutable
public abstract class IcebergTableMerge {
  public abstract String tableName();

  public abstract CatalogTableOperations ops();

  public abstract TableMetadata mergeBaseTableMetadata();

  public abstract TableMetadata sourceTableMetadata();

  public abstract TableMetadata targetTableMetadata();

  @Value.Derived
  public FileIO fileIO() {
    return ops().io();
  }

  public static ImmutableIcebergTableMerge.Builder builder() {
    return ImmutableIcebergTableMerge.builder();
  }

  public Optional<TableMetadata> merge() {
    checkArgument(
        sourceTableMetadata().uuid().equals(targetTableMetadata().uuid()),
        "Iceberg tables have different Iceberg table UUIDs, source table UUID is %s, target table UUID is %s",
        sourceTableMetadata().uuid(),
        targetTableMetadata().uuid());
    checkArgument(
        sourceTableMetadata().uuid().equals(mergeBaseTableMetadata().uuid()),
        "Iceberg tables have different Iceberg table UUIDs, source table UUID is %s, merge base table UUID is %s",
        sourceTableMetadata().uuid(),
        targetTableMetadata().uuid());

    Snapshot mergeBaseSnapshot = mergeBaseTableMetadata().currentSnapshot();
    long mergeBaseSnapshotId = mergeBaseSnapshot.snapshotId();
    checkArgument(
        sourceTableMetadata().snapshot(mergeBaseSnapshotId) != null,
        "Source table metadata does not contain merge-base snapshot ID %s",
        mergeBaseSnapshotId);
    checkArgument(
        targetTableMetadata().snapshot(mergeBaseSnapshotId) != null,
        "Target table metadata does not contain merge-base snapshot ID %s",
        mergeBaseSnapshotId);

    // First, build an "intermediate" snapshot on top of the target - based on the _diff_ of the
    // merge-base and source. This shall apply the _changes_ made on source onto target.
    Optional<TableMetadata> intermediateMetadata =
        ImmutableIntermediateMetadata.builder()
            .mergeBaseTableMetadata(mergeBaseTableMetadata())
            .sourceTableMetadata(sourceTableMetadata())
            .targetTableMetadata(targetTableMetadata())
            .build()
            .intermediateTableMetadata();

    // TODO see whether we can add a manifest-file cache to avoid reading the same manifests from
    //  the merge-base, the merge-from and merge-target.
    // TODO see whether org.apache.iceberg.ManifestFiles#CONTENT_CACHES is sufficient,
    //  see CatalogProperties.IO_MANIFEST_CACHE_* properties for the FileIO.

    // Computes a diff between all data+delete files on the merge-base and the source/target.
    AllFiles mergeBaseFiles = new AllFiles(fileIO(), mergeBaseTableMetadata());

    // Inspect manifests (data files/changes) on latest snapshot on source metadata.
    FileChanges sourceFileChanges = mergeBaseFiles.computeChanges(sourceTableMetadata());

    // Short-cut if there are no file changes, we can return the possibly changed table-metadata
    // immediately.
    if (sourceFileChanges.isEmpty()) {
      return intermediateMetadata;
    }

    // Cannot compute the diff from merge-base to merge-target from the snapshots, need to
    // inspect all manifests.
    FileChanges targetFileChanges =
        new FileChanges(fileIO(), mergeBaseFiles, targetTableMetadata());

    // At this point we have in `sourceFileChanges`/`targetFileChanges`:
    // - the data files that have been added on source
    //   --> in `FileChanges.addedDataFiles`
    // - the data files that exist on target and have been deleted on source
    //   --> in `FileChanges.removedDataFiles`
    // - similar for delete-files

    FileChanges changesDiff = sourceFileChanges.computeDiffToTarget(targetFileChanges);

    // TODO Re-arrange sequence numbers

    // TODO which combinations of added/removed data/delete files on source/target shall be
    //  rejected? Examples:
    //  - delete files added to a data file on one side, data file deleted on the other side
    // TODO Controversial approach: assume that the target-side changes are always fine?

    // At this point we have all changes that need to be applied onto the target.
    // Duplicate file additions & removals have been eliminated in `changesDiff`, so in case the
    // same data or delete files have been added/removed on both the source and the target, these
    // will not appear as "changes to apply".
    // For other rules, see  FileChanges`.

    if (changesDiff.isEmpty()) {
      return Optional.empty();
    }

    // Use the (possibly updated) target table metadata as the current metadata for Iceberg's
    // `TableOperations`.
    TableMetadata currentTargetMetadata = intermediateMetadata.orElse(targetTableMetadata());
    ops().setCurrent(currentTargetMetadata);

    // Last, apply the data file operations.
    TableMetadata finalMetadata = applyDataFileChanges(changesDiff);

    return Optional.of(finalMetadata);
  }

  private TableMetadata applyDataFileChanges(FileChanges changes) {

    // ABOUT SEQUENCE NUMBERS:

    // From https://github.com/apache/iceberg/issues/358:
    // The scope of a delete file will be limited in 2 ways: by time and by partition. Sequence
    // numbers will be used to implement the first. All data and delete files will be added with a
    // sequence number and deletes apply to any data file with a sequence number less than or equal
    // to the delete's sequence number.
    //
    // ... delete not applied to the data file with a sequence number equal to the delete's sequence
    // number,
    //
    // ... a delete should not be applied to a file with the same sequence number. An upsert would
    // add a delete file and a data file with the same sequence number, one containing the row-level
    // delete and one containing the upserted row content. Not applying deletes with the same
    // sequence number ensures that the upsert doesn't delete itself.
    //
    // From https://github.com/apache/iceberg/issues/360:
    // Position deletes would apply to data files with a sequence number <= to the delete file's
    // sequence number. They would still be used for other delete cases, which require <.
    //
    // If we want to use equality delete files for this, then we would similarly apply an equality
    // delete file when a data file's sequence number is <= the equality delete's sequence number.
    //
    // The optimization I was suggesting is if we don't use equality deletes to encode deletes
    // within the same commit, we can use < instead of <=.

    BaseTable table = new BaseTable(ops(), tableName());

    // TODO maybe need to respect and adopt sequence numbers when we write our own manifest instead
    //  of using the AppendFiles/DeleteFiles snapshot-update operations
    //
    // long mergeBaseLastSequenceNumber = mergeBaseTableMetadata().lastSequenceNumber();
    //
    // PartitionSpec spec;
    // OutputFile file;
    // Long snapshotId;
    //
    // ManifestWriter<DataFile> dataManifestWriter =
    //    ManifestFiles.write(targetTableMetadata().formatVersion(), spec, file, snapshotId);
    // try {
    //  dataManifestWriter.add(dataFile, dataSequenceNumber);
    //  dataManifestWriter.delete(dataFile, dataSequenceNumber, fileSequenceNumber);
    // } finally {
    //  dataManifestWriter.close();
    // }
    // ManifestFile dataManifest = dataManifestWriter.toManifestFile();
    // Metrics dataWriterMetrics = dataManifestWriter.metrics();
    //
    // ManifestWriter<DeleteFile> deleteManifestWriter =
    //    ManifestFiles.writeDeleteManifest(
    //        targetTableMetadata().formatVersion(), spec, file, snapshotId);
    // try {
    //  deleteManifestWriter.add(deleteFile, dataSequenceNumber);
    //  deleteManifestWriter.delete(deleteFile, dataSequenceNumber, fileSequenceNumber);
    // } finally {
    //  deleteManifestWriter.close();
    // }
    // ManifestFile deleteManifest = deleteManifestWriter.toManifestFile();
    // Metrics deleteWriterMetrics = dataManifestWriter.metrics();

    // Data files

    // TODO should use only one 'AppendFiles' snapshot-update here and add generated manifest-files.
    //  This helps to have one new snapshot for the merge operation. Note that the metadata-update
    //  (the intermediate metadata) might still create a preceding snapshot.
    //  - need to adjust the data-sequence-numbers, if present:
    //    - data-sequence-numbers that refer to the sequence-numbers from the
    //      merge-base-->merge-source diff must be adjusted to the _new_ sequence number for the
    //      new snapshot on the merge-target

    if (!changes.removedDataFiles.isEmpty()) {
      // Data files have been removed, go ahead and delete the files (per partition spec).
      // TODO see notes about generating manifest files above

      // TODO validate that this works for multiple partition specs
      for (Map.Entry<Integer, List<DataFile>> dataFilesPerSpec :
          changes.removedDataFiles.values().stream()
              .collect(Collectors.groupingBy(DataFile::specId))
              .entrySet()) {
        DeleteFiles deleteFiles = table.newDelete();
        dataFilesPerSpec.getValue().forEach(deleteFiles::deleteFile);
        deleteFiles.commit();
      }
    }

    if (!changes.addedDataFiles.isEmpty()) {
      // Data files have been appended, go ahead and append the files (per partition spec).
      // TODO see notes about generating manifest files above

      // org.apache.iceberg.MergingSnapshotProducer.appendFile() throws a `ValidationException`
      // in `setDataSpec`, if more than one specId is used. This is not intuitive, because the
      // docs of o.a.i.AppendFiles do not mention this restriction, but only say:
      // "Append implementation that produces a minimal number of manifest files."
      // TODO validate that this works properly with multiple partition specs
      for (Map.Entry<Integer, List<DataFile>> dataFilesPerSpec :
          changes.addedDataFiles.values().stream()
              .collect(Collectors.groupingBy(DataFile::specId))
              .entrySet()) {
        // TODO overwritten files? need to at least detect and reject overwrites and rewrites!
        // TODO rewritten files? need to at least detect and reject overwrites and rewrites!

        AppendFiles appendFiles = table.newAppend();
        dataFilesPerSpec.getValue().forEach(appendFiles::appendFile);
        appendFiles.commit();
      }
    }

    // Delete files

    if (!changes.removedDeleteFiles.isEmpty()) {
      throw new UnsupportedOperationException(
          "Delete files removed - need to write our own manifest");
      // TODO handle delete files, too - see notes about generating manifest files above
    }

    if (!changes.addedDeleteFiles.isEmpty()) {
      throw new UnsupportedOperationException(
          "Delete files added - need to write our own manifest");
      // TODO handle delete files, too - see notes about generating manifest files above
    }

    return ops().current();
  }
}

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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.immutables.value.Value;

/**
 * Builds an (intermediate) snapshot with all the changes except those for data files. Updates the
 * table-metadata properties, adds missing schemas, partition-specs, sort-orders, sets
 * current/default schema/partition-spec/sort-order, etc.
 */
@Value.Immutable
abstract class IntermediateMetadata {
  abstract TableMetadata mergeBaseTableMetadata();

  abstract TableMetadata sourceTableMetadata();

  abstract TableMetadata targetTableMetadata();

  @Value.Derived
  Optional<TableMetadata> intermediateTableMetadata() {
    Map<Integer, Schema> knownSchemas = new HashMap<>(targetTableMetadata().schemasById());
    Map<Integer, PartitionSpec> knownSpecs = new HashMap<>(targetTableMetadata().specsById());
    Map<Integer, SortOrder> knownSortOrders = new HashMap<>(targetTableMetadata().sortOrdersById());
    int lastColumnId = targetTableMetadata().lastColumnId();

    TableMetadata.Builder newMetadataBuilder = TableMetadata.buildFrom(targetTableMetadata());

    boolean changes = false;

    // build diff of source-properties to target-properties and apply
    MapDifference<String, String> propertiesDiff =
        Maps.difference(sourceTableMetadata().properties(), mergeBaseTableMetadata().properties());
    if (!propertiesDiff.areEqual()) {
      changes = true;
      newMetadataBuilder.removeProperties(propertiesDiff.entriesOnlyOnRight().keySet());
      newMetadataBuilder.setProperties(propertiesDiff.entriesOnlyOnLeft());
      newMetadataBuilder.setProperties(
          propertiesDiff.entriesDiffering().entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().leftValue())));
    }

    // add missing schemas to target
    // TODO need to add all new schemas from the source or just the schemas referenced via the
    //   manifests/data-files? This is tricky/ambiguous:
    //   - One might expect that only the "needed" schemas are applied to the target.
    //   - One might expect that all changes are applied to the target (e.g. a branch that only
    //     added a schema).
    for (Schema schema : sourceTableMetadata().schemas()) {
      int id = schema.schemaId();
      // only consider schemas that have been added to the source since the merge-base
      if (!mergeBaseTableMetadata().schemasById().containsKey(id)) {
        Schema known = knownSchemas.get(id);
        if (known == null) {
          // TODO add validation that there are no conflicting column-ids
          changes = true;
          lastColumnId = Math.max(schema.highestFieldId(), lastColumnId);
          newMetadataBuilder.addSchema(schema, lastColumnId);
          knownSchemas.put(id, schema);
        } else {
          // TODO add functionality merge the schema change, if possible
          checkState(
              schema.equals(known),
              "Schema with ID %s to merge is already present on the target table as a different schema",
              id);
        }
      }
    }
    // add missing partition-specs to target
    // TODO need to add all new partition-specs from the source or just the partition-specs
    //   referenced via the manifests/data-files? This is tricky/ambiguous:
    //   - One might expect that only the "needed" specs are applied to the target.
    //   - One might expect that all changes are applied to the target (e.g. a branch that only
    //     added a schema).
    for (PartitionSpec spec : sourceTableMetadata().specs()) {
      int id = spec.specId();
      // only consider partition-specs that have been added to the source since the merge-base
      if (!mergeBaseTableMetadata().specsById().containsKey(id)) {
        PartitionSpec known = knownSpecs.get(id);
        if (known == null) {
          changes = true;
          newMetadataBuilder.addPartitionSpec(spec);
          knownSpecs.put(id, spec);
        } else {
          checkState(
              spec.equals(known),
              "Partition spec with ID %s to merge is already present on the target table as a different partition spec",
              id);
        }
      }
    }
    // add missing sort-orders to target
    // TODO need to add all new sort-orders from the source or just the sort-orders referenced via
    //   the manifests/data-files? This is tricky/ambiguous:
    //   - One might expect that only the "needed" sort-orders are applied to the target.
    //   - One might expect that all changes are applied to the target (e.g. a branch that only
    //     added a schema).
    for (SortOrder sortOrder : sourceTableMetadata().sortOrders()) {
      int id = sortOrder.orderId();
      // only consider sort-orders that have been added to the source since the merge-base
      if (!mergeBaseTableMetadata().sortOrdersById().containsKey(id)) {
        SortOrder known = knownSortOrders.get(id);
        if (known == null) {
          changes = true;
          newMetadataBuilder.addSortOrder(sortOrder);
          knownSortOrders.put(id, sortOrder);
        } else {
          checkState(
              sortOrder.equals(known),
              "Sort order with ID %s to merge is already present on the target table as a different sort order",
              id);
        }
      }
    }

    if (sourceTableMetadata().currentSchemaId() != mergeBaseTableMetadata().currentSchemaId()) {
      // Update the current schema ID only if it changed between merge-base & source
      changes = true;
      newMetadataBuilder.setCurrentSchema(sourceTableMetadata().currentSchemaId());
    }
    if (sourceTableMetadata().defaultSortOrderId()
        != mergeBaseTableMetadata().defaultSortOrderId()) {
      // Update the default sort order ID only if it changed between merge-base & source
      changes = true;
      newMetadataBuilder.setDefaultSortOrder(sourceTableMetadata().defaultSortOrderId());
    }
    if (sourceTableMetadata().defaultSpecId() != mergeBaseTableMetadata().defaultSpecId()) {
      // Update the default spec ID only if it changed between merge-base & source
      changes = true;
      newMetadataBuilder.setDefaultPartitionSpec(sourceTableMetadata().defaultSpecId());
    }

    // TODO do we need the changes from all intermediate table-metadata objects??
    sourceTableMetadata().changes();

    // NOTE: we ignore Iceberg refs in Nessie
    sourceTableMetadata().refs();

    // TODO probably need to "clear" the list of statistics files, because those probably do not
    //  match anymore
    sourceTableMetadata().statisticsFiles();

    return changes ? Optional.of(newMetadataBuilder.build()) : Optional.empty();
  }
}

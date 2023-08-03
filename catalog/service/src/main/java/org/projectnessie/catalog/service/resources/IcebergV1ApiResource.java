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
package org.projectnessie.catalog.service.resources;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.projectnessie.catalog.content.iceberg.metadata.Json.OBJECT_MAPPER;
import static org.projectnessie.catalog.content.iceberg.metadata.Json.jsonToTableMetadata;
import static org.projectnessie.catalog.content.iceberg.metadata.Json.partitionSpecToJsonMap;
import static org.projectnessie.catalog.content.iceberg.metadata.Json.schemaToJsonMap;
import static org.projectnessie.catalog.content.iceberg.metadata.Json.snapshotToJsonMap;
import static org.projectnessie.catalog.content.iceberg.metadata.Json.snapshotUpdateSummaryToJsonMap;
import static org.projectnessie.catalog.content.iceberg.metadata.Json.sortOrderToJsonMap;
import static org.projectnessie.model.metadata.GenericContentMetadata.genericContentMetadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.enterprise.context.RequestScoped;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdate.SetProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest.ReportType;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.api.IcebergV1Api;
import org.projectnessie.catalog.api.errors.IcebergConflictException;
import org.projectnessie.catalog.service.spi.DecodedPrefix;
import org.projectnessie.catalog.service.spi.NamespaceRef;
import org.projectnessie.catalog.service.spi.TableRef;
import org.projectnessie.catalog.service.spi.Warehouse;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.api.UpdateNamespaceResult;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;

@RequestScoped
@jakarta.enterprise.context.RequestScoped
public class IcebergV1ApiResource extends BaseIcebergResource implements IcebergV1Api {

  @Override
  public ListNamespacesResponse listNamespaces(String prefix, String parent) throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, parent);

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    ListNamespacesResponse.Builder response = ListNamespacesResponse.builder();

    api
        .getMultipleNamespaces()
        .refName(namespaceRef.referenceName())
        .hashOnRef(namespaceRef.hashWithRelativeSpec())
        .namespace(namespaceRef.namespace())
        .onlyDirectChildren(true)
        .get()
        .getNamespaces()
        .stream()
        .map(this::toIcebergNamespace)
        .forEach(response::add);

    return response.build();
  }

  @Override
  public CreateNamespaceResponse createNamespace(
      String prefix, CreateNamespaceRequest createNamespaceRequest) throws IOException {
    ParsedReference ref = decodePrefix(prefix).parsedReference();

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    Namespace ns =
        api.createNamespace()
            .refName(ref.name())
            .hashOnRef(ref.hashWithRelativeSpec())
            .namespace(toNessieNamespace(createNamespaceRequest.namespace()))
            .properties(createNamespaceRequest.properties())
            .create();

    return CreateNamespaceResponse.builder()
        .withNamespace(createNamespaceRequest.namespace())
        .setProperties(ns.getProperties())
        .build();
  }

  @Override
  public GetNamespaceResponse loadNamespaceMetadata(String prefix, String namespace)
      throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();
    Namespace ns =
        api.getNamespace()
            .refName(namespaceRef.referenceName())
            .hashOnRef(namespaceRef.hashWithRelativeSpec())
            .namespace(namespaceRef.namespace())
            .get();

    return GetNamespaceResponse.builder()
        .withNamespace(toIcebergNamespace(ns))
        .setProperties(ns.getProperties())
        .build();
  }

  @Override
  public void dropNamespace(String prefix, String namespace) throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    api.deleteNamespace()
        .refName(namespaceRef.referenceName())
        .hashOnRef(namespaceRef.hashWithRelativeSpec())
        .namespace(namespaceRef.namespace())
        .delete();
  }

  @Override
  public UpdateNamespacePropertiesResponse updateProperties(
      String prefix,
      String namespace,
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest)
      throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    UpdateNamespaceResult namespaceUpdate =
        api.updateProperties()
            .refName(namespaceRef.referenceName())
            .hashOnRef(namespaceRef.hashWithRelativeSpec())
            .namespace(namespaceRef.namespace())
            .updateProperties(updateNamespacePropertiesRequest.updates())
            .removeProperties(new HashSet<>(updateNamespacePropertiesRequest.removals()))
            .updateWithResponse();

    UpdateNamespacePropertiesResponse.Builder response =
        UpdateNamespacePropertiesResponse.builder();

    Map<String, String> oldProperties = namespaceUpdate.getNamespaceBeforeUpdate().getProperties();
    Map<String, String> newProperties = namespaceUpdate.getNamespace().getProperties();

    oldProperties.keySet().stream()
        .filter(k -> !newProperties.containsKey(k))
        .forEach(response::addRemoved);

    Stream.concat(
            updateNamespacePropertiesRequest.removals().stream(),
            updateNamespacePropertiesRequest.updates().keySet().stream())
        .filter(k -> !oldProperties.containsKey(k))
        .forEach(response::addMissing);

    newProperties.entrySet().stream()
        .filter(
            e -> {
              String newValue = oldProperties.get(e.getKey());
              return !e.getValue().equals(newValue);
            })
        .map(Map.Entry::getKey)
        .forEach(response::addUpdated);

    return response.build();
  }

  @Override
  public ListTablesResponse listTables(String prefix, String namespace) throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    ListTablesResponse.Builder response = ListTablesResponse.builder();

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    // TODO this deserves some REST API enhancement to retrieve all content-keys + contents
    //  (paged/streaming)
    api
        .getEntries()
        .refName(namespaceRef.referenceName())
        .hashOnRef(namespaceRef.hashWithRelativeSpec())
        .filter(
            format("entry.encodedKey.startsWith('%s.')", namespaceRef.namespace().toPathString()))
        .stream()
        .map(e -> TableIdentifier.of(e.getName().getElementsArray()))
        .forEach(response::add);

    return response.build();
  }

  @Override
  public LoadTableResponse createTable(
      String prefix, String namespace, CreateTableRequest createTableRequest)
      throws IOException, IcebergConflictException {
    TableRef tableRef = decodeTableRef(prefix, namespace, createTableRequest.name());
    Warehouse warehouse = tenantSpecific.getWarehouse(tableRef.warehouse());

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    GetMultipleContentsResponse tableCheck =
        api.getContent()
            .refName(tableRef.referenceName())
            .key(tableRef.contentKey())
            .getWithResponse();
    Branch ref = checkBranch(tableCheck.getEffectiveReference());

    Content c = tableCheck.toContentsMap().get(tableRef.contentKey());
    if (c != null) {
      verifyIcebergTable(c);
      throw new IcebergConflictException(
          "AlreadyExistsException", "Table already exists: " + tableRef.contentKey());
    }

    SortOrder sortOrder = createTableRequest.writeOrder();
    if (sortOrder == null) {
      sortOrder = SortOrder.unsorted();
    }
    PartitionSpec spec = createTableRequest.spec();
    if (spec == null) {
      spec = PartitionSpec.unpartitioned();
    }
    Schema schema = createTableRequest.schema();
    String location = createTableRequest.location();
    if (location == null) {
      location =
          tenantSpecific.defaultTableLocation(
              createTableRequest.location(),
              tableRef,
              ref,
              tenantSpecific.getWarehouse(tableRef.warehouse()));
    }

    Map<String, String> properties = new HashMap<>(createTableRequest.properties());
    String expectedCommitId = properties.remove(PROPERTY_STAGED_COMMIT);

    TableMetadata table;

    if (createTableRequest.stageCreate()) {
      checkArgument(expectedCommitId == null, "Table already staged.");
      table = newTableMetadata(schema, spec, sortOrder, location, properties);
    } else {
      checkArgument(
          expectedCommitId == null, "Creating a staged table must be committed via update-table.");

      table = newTableMetadata(schema, spec, sortOrder, location, properties);
      table = warehouse.metadataIO().store(table, 0);

      ImmutablePut.Builder putOperation = Operation.Put.builder().key(tableRef.contentKey());
      updateIcebergTable(table, null, putOperation);
      CommitResponse commit =
          api.commitMultipleOperations()
              .branch(ref)
              .commitMeta(
                  tenantSpecific.buildCommitMeta(format("Create table %s", tableRef.contentKey())))
              .operation(putOperation.build())
              .commitWithResponse();

      ref = commit.getTargetBranch();
    }

    return buildLoadTableResponse(table, ref, createTableRequest.stageCreate());
  }

  @Override
  public LoadTableResponse registerTable(
      String prefix, String namespace, RegisterTableRequest registerTableRequest)
      throws IOException, IcebergConflictException {
    TableRef tableRef = decodeTableRef(prefix, namespace, registerTableRequest.name());
    Warehouse warehouse = tenantSpecific.getWarehouse(tableRef.warehouse());

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    GetMultipleContentsResponse tableCheck =
        api.getContent()
            .refName(tableRef.referenceName())
            .key(tableRef.contentKey())
            .getWithResponse();
    Branch ref = checkBranch(tableCheck.getEffectiveReference());

    Content c = tableCheck.toContentsMap().get(tableRef.contentKey());
    if (c != null) {
      verifyIcebergTable(c);
      throw new IcebergConflictException(
          "AlreadyExistsException", "Table already exists: " + tableRef.contentKey());
    }

    TableMetadata table = warehouse.metadataIO().load(registerTableRequest.metadataLocation());

    ImmutablePut.Builder putOperation = Operation.Put.builder().key(tableRef.contentKey());
    updateIcebergTable(table, null, putOperation);
    api.commitMultipleOperations()
        .branch(ref)
        .commitMeta(
            tenantSpecific.buildCommitMeta(format("Create table %s", tableRef.contentKey())))
        .operation(putOperation.build())
        .commitWithResponse();

    return buildLoadTableResponse(table, ref, false);
  }

  @Override
  public LoadTableResponse loadTable(String prefix, String namespace, String table)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);
    Warehouse warehouse = tenantSpecific.getWarehouse(tableRef.warehouse());

    ContentResponse content = fetchIcebergTable(tableRef);
    IcebergTable icebergTable = verifyIcebergTable(content.getContent());
    Reference reference = requireNonNull(content.getEffectiveReference());
    TableMetadata metadata = warehouse.metadataIO().loadAndFixTableMetadata(icebergTable);

    return buildLoadTableResponse(metadata, reference, false);
  }

  @Override
  public LoadTableResponse updateTable(
      String prefix, String namespace, String table, UpdateTableRequest commitTableRequest)
      throws IOException, IcebergConflictException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);
    Warehouse warehouse = tenantSpecific.getWarehouse(tableRef.warehouse());

    // fetch existing table

    IcebergTable icebergTable = null;
    Branch ref = null;
    TableMetadata base = null;
    List<UpdateRequirement> requirements = commitTableRequest.requirements();
    NessieNotFoundException notFound = null;
    int newVersion = 0;
    try {
      ContentResponse content = fetchIcebergTable(tableRef);
      icebergTable = verifyIcebergTable(content.getContent());
      newVersion = warehouse.metadataIO().parseVersion(icebergTable.getMetadataLocation()) + 1;
      ref = checkBranch(content.getEffectiveReference());
      base = warehouse.metadataIO().loadAndFixTableMetadata(icebergTable);
    } catch (NessieNotFoundException e) {
      // paranoia...
      if (requirements == null || requirements.isEmpty()) {
        requirements = UpdateTableRequest.builderForCreate().build().requirements();
      }
      notFound = e;
    }

    // check requirements

    List<String> failedRequirements = new ArrayList<>();
    for (UpdateRequirement requirement : requirements) {
      try {
        requirement.validate(base);
      } catch (CommitFailedException failed) {
        failedRequirements.add(failed.getMessage());
      }
    }
    if (!failedRequirements.isEmpty()) {
      throw new IcebergConflictException("CommitFailedException", join(", ", failedRequirements));
    }

    // update tables

    TableMetadata.Builder metadataBuilder =
        base != null ? TableMetadata.buildFrom(base) : TableMetadata.buildFromEmpty();
    String stagedCommit = null;
    for (MetadataUpdate update : commitTableRequest.updates()) {
      if (update instanceof SetProperties) {
        SetProperties setProperties = (SetProperties) update;
        Map<String, String> properties = new HashMap<>(setProperties.updated());
        if (properties.containsKey(PROPERTY_STAGED_COMMIT)) {
          stagedCommit = properties.remove(PROPERTY_STAGED_COMMIT);
        }
        // remove the following, just in case
        properties.keySet().removeAll(CHANGING_PROPERTIES);

        if (properties.isEmpty()) {
          continue;
        }
      }

      // TODO prevent any Iceberg branch-snapshot-ref changes to something else than "main":
      //      metadataBuilder.setBranchSnapshot(table.getSnapshotId(), SnapshotRef.MAIN_BRANCH);

      update.applyTo(metadataBuilder);
    }
    TableMetadata updatedMetadata = metadataBuilder.build();

    // TODO ensure that the branch-snapshot points to Iceberg's "main":
    //      metadataBuilder.setBranchSnapshot(table.getSnapshotId(), SnapshotRef.MAIN_BRANCH);

    // compute final ref to commit to

    if (ref == null) {
      // CREATE table
      if (stagedCommit == null) {
        // The 'new NessieNotFoundException' can never be triggered due to the control flow
        // above, but this makes the compiler happy.
        throw notFound;
      }

      ref = Branch.of(tableRef.referenceName(), stagedCommit);
    } else {
      checkArgument(
          stagedCommit == null,
          "Got a staged create-table commit for a table that already exists.");
    }

    TableMetadata storedMetadata = warehouse.metadataIO().store(updatedMetadata, newVersion);
    ImmutablePut.Builder putOperation = Operation.Put.builder().key(tableRef.contentKey());
    updateIcebergTable(storedMetadata, icebergTable, putOperation);

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();
    CommitResponse commit =
        api.commitMultipleOperations()
            .branch(ref)
            .commitMeta(
                tenantSpecific.buildCommitMeta(format("Update table %s", tableRef.contentKey())))
            .operation(putOperation.build())
            .commitWithResponse();

    return buildLoadTableResponse(storedMetadata, commit.getTargetBranch(), false);
  }

  private static class TableToCommit {
    final UpdateTableRequest tableChange;
    final ContentKey key;
    IcebergTable content;
    TableMetadata base;
    int newVersion;
    TableMetadata updatedMetadata;
    TableMetadata storedMetadata;

    TableToCommit(UpdateTableRequest tableChange, ContentKey key) {
      this.tableChange = tableChange;
      this.key = key;
    }

    ContentKey key() {
      return key;
    }
  }

  @Override
  public void commitTransaction(String prefix, CommitTransactionRequest commitTransactionRequest)
      throws IOException, IcebergConflictException {
    DecodedPrefix decodedPrefix = decodePrefix(prefix);
    ParsedReference parsedRef = decodedPrefix.parsedReference();
    Warehouse warehouse = tenantSpecific.getWarehouse(decodedPrefix.warehouse());

    // collect tables by content-key

    Map<ContentKey, TableToCommit> tablesToCommit =
        commitTransactionRequest.tableChanges().stream()
            .map(
                tableChange -> {
                  TableRef tableReference = decodeTableRef(prefix, tableChange.identifier());
                  ContentKey contentKey = tableReference.contentKey();
                  return new TableToCommit(tableChange, contentKey);
                })
            .collect(Collectors.toMap(TableToCommit::key, Function.identity()));

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    // fetch existing table

    GetContentBuilder getContents =
        api.getContent().refName(parsedRef.name()).hashOnRef(parsedRef.hashWithRelativeSpec());
    tablesToCommit.keySet().stream().forEach(getContents::key);
    GetMultipleContentsResponse contentsResponse = getContents.getWithResponse();
    Branch ref = checkBranch(contentsResponse.getEffectiveReference());
    for (GetMultipleContentsResponse.ContentWithKey contentWithKey :
        contentsResponse.getContents()) {
      Content content = contentWithKey.getContent();
      if (content.getType() != Content.Type.ICEBERG_TABLE) {
        // TODO do something here
      }
      IcebergTable icebergTable = verifyIcebergTable(content);
      TableToCommit tableToCommit = tablesToCommit.get(contentWithKey.getKey());

      tableToCommit.content = icebergTable;
      tableToCommit.base = warehouse.metadataIO().loadAndFixTableMetadata(icebergTable);
      tableToCommit.newVersion =
          warehouse.metadataIO().parseVersion(icebergTable.getMetadataLocation()) + 1;
    }

    // check requirements

    List<String> failedRequirements = new ArrayList<>();
    for (TableToCommit tableToCommit : tablesToCommit.values()) {
      for (UpdateRequirement requirement : tableToCommit.tableChange.requirements()) {
        try {
          requirement.validate(tableToCommit.base);
        } catch (CommitFailedException failed) {
          failedRequirements.add(failed.getMessage());
        }
      }
    }
    if (!failedRequirements.isEmpty()) {
      throw new IcebergConflictException("CommitFailedException", join(", ", failedRequirements));
    }

    // update tables

    for (TableToCommit tableToCommit : tablesToCommit.values()) {
      TableMetadata.Builder metadataBuilder =
          tableToCommit.base != null
              ? TableMetadata.buildFrom(tableToCommit.base)
              : TableMetadata.buildFromEmpty();
      for (MetadataUpdate update : tableToCommit.tableChange.updates()) {
        if (update instanceof SetProperties) {
          SetProperties setProperties = (SetProperties) update;
          Map<String, String> properties = new HashMap<>(setProperties.updated());
          // remove the following, just in case
          properties.keySet().removeAll(CHANGING_PROPERTIES);

          if (properties.isEmpty()) {
            continue;
          }
        }

        // TODO prevent any Iceberg branch-snapshot-ref changes to something else than "main":
        //      metadataBuilder.setBranchSnapshot(table.getSnapshotId(), SnapshotRef.MAIN_BRANCH);

        update.applyTo(metadataBuilder);
      }

      // TODO ensure that the branch-snapshot points to Iceberg's "main":
      //      metadataBuilder.setBranchSnapshot(table.getSnapshotId(), SnapshotRef.MAIN_BRANCH);

      tableToCommit.updatedMetadata = metadataBuilder.build();
    }

    // store metadata files and generate put-operations

    CommitMultipleOperationsBuilder commitBuilder =
        api.commitMultipleOperations()
            .branch(ref)
            .commitMeta(
                tenantSpecific.buildCommitMeta(
                    format(
                        "Update tables %s",
                        tablesToCommit.keySet().stream()
                            .map(ContentKey::toString)
                            .collect(Collectors.joining(", ")))));
    for (TableToCommit tableToCommit : tablesToCommit.values()) {
      tableToCommit.storedMetadata =
          warehouse.metadataIO().store(tableToCommit.updatedMetadata, tableToCommit.newVersion);
      ImmutablePut.Builder putOperation = Operation.Put.builder().key(tableToCommit.key);
      updateIcebergTable(tableToCommit.storedMetadata, tableToCommit.content, putOperation);
      commitBuilder.operation(putOperation.build());
    }
    commitBuilder.commitWithResponse();
  }

  private void updateIcebergTable(
      TableMetadata metadata,
      IcebergTable existingIcebergTable,
      ImmutablePut.Builder putOperation) {
    ImmutableIcebergTable.Builder builder = IcebergTable.builder();
    if (existingIcebergTable != null) {
      builder.from(existingIcebergTable);
    }
    Snapshot snapshot = metadata.currentSnapshot();
    long snapshotId = snapshot != null ? snapshot.snapshotId() : -1L;
    int schemaId = metadata.currentSchemaId();
    int specId = metadata.defaultSpecId();
    int sortOrderId = metadata.defaultSortOrderId();

    // TODO Probably only the "updateSummary" makes sense.
    if (snapshot != null) {
      if (metadata.formatVersion() >= 2) {
        // Only push the "full" snapshot object, if the format-version is at least 2, so without
        // the "embedded" manifest/-lists, which are extremely large.
        putOperation.addMetadata(
            genericContentMetadata("iceberg.snapshot", snapshotToJsonMap(snapshot)));
      } else {
        // For format-version < 2, push an "update-summary", which contains the summary
        putOperation.addMetadata(
            genericContentMetadata(
                "iceberg.updateSummary", snapshotUpdateSummaryToJsonMap(snapshot)));
      }
    }
    Schema schema = metadata.schemasById().get(schemaId);
    if (schema != null) {
      putOperation.addMetadata(genericContentMetadata("iceberg.schema", schemaToJsonMap(schema)));
    }
    PartitionSpec spec = metadata.spec(specId);
    if (spec != null) {
      putOperation.addMetadata(
          genericContentMetadata("iceberg.spec", partitionSpecToJsonMap(spec)));
    }
    SortOrder sortOrder = metadata.sortOrdersById().get(sortOrderId);
    if (sortOrder != null) {
      putOperation.addMetadata(
          genericContentMetadata("iceberg.sortOrder", sortOrderToJsonMap(sortOrder)));
    }

    builder
        .metadataLocation(metadata.metadataFileLocation())
        .snapshotId(snapshotId)
        .schemaId(schemaId)
        .specId(specId)
        .sortOrderId(sortOrderId);

    putOperation.content(builder.build());
  }

  @Override
  public void dropTable(String prefix, String namespace, String table, Boolean purgeRequested)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    ContentResponse fetchIcebergTable = fetchIcebergTable(tableRef);
    Branch ref = checkBranch(fetchIcebergTable.getEffectiveReference());

    api.commitMultipleOperations()
        .branch(ref)
        .commitMeta(tenantSpecific.buildCommitMeta(format("drop table %s", tableRef.contentKey())))
        .operation(Operation.Delete.of(tableRef.contentKey()))
        .commitWithResponse();
  }

  @Override
  public void tableExists(String prefix, String namespace, String table) throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();
    api.getContent()
        .refName(tableRef.referenceName())
        .hashOnRef(tableRef.referenceHash())
        .getSingle(tableRef.contentKey());
  }

  @Override
  public void renameTable(String prefix, RenameTableRequest renameTableRequest)
      throws IOException, IcebergConflictException {
    TableRef fromTableRef = decodeTableRef(prefix, renameTableRequest.source());
    TableRef toTableRef = decodeTableRef(prefix, renameTableRequest.destination());

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    ContentResponse fromTable =
        api.getContent()
            .refName(fromTableRef.referenceName())
            .hashOnRef(fromTableRef.referenceHash())
            .getSingle(fromTableRef.contentKey());

    try {
      api.getContent()
          .refName(fromTableRef.referenceName())
          .hashOnRef(fromTableRef.referenceHash())
          .getSingle(toTableRef.contentKey());
      throw new IcebergConflictException(
          "AlreadyExistsException", "Table already exists: " + toTableRef.contentKey());
    } catch (NessieNotFoundException e) {
      // good!
    }

    Reference effectiveRef = fromTable.getEffectiveReference();
    checkArgument(
        effectiveRef instanceof Branch,
        format("Must only rename a table on a branch, but got %s", effectiveRef));

    api.commitMultipleOperations()
        .branch((Branch) effectiveRef)
        .commitMeta(
            tenantSpecific.buildCommitMeta(
                format(
                    "rename table %s to %s", fromTableRef.contentKey(), toTableRef.contentKey())))
        .operation(Operation.Delete.of(fromTableRef.contentKey()))
        .operation(Operation.Put.of(toTableRef.contentKey(), fromTable.getContent()))
        .commitWithResponse();
  }

  @Override
  public void reportMetrics(
      String prefix, String namespace, String table, ReportMetricsRequest reportMetricsRequest)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();
    ContentResponse content =
        api.getContent()
            .refName(tableRef.referenceName())
            .hashOnRef(tableRef.referenceHash())
            .getSingle(tableRef.contentKey());

    // Using the effective reference from ContentResponse would be wrong here, because we do not
    // know the commit ID for/on which the metrics were generated, unless the hash is included in
    // TableRef.

    pushMetrics(
        tableRef,
        content.getContent(),
        reportMetricsRequest.reportType(),
        reportMetricsRequest.report());
  }

  private void pushMetrics(
      TableRef tableRef, Content content, ReportType reportType, MetricsReport report) {
    // TODO push metrics through
  }

  private LoadTableResponse buildLoadTableResponse(
      TableMetadata table, Reference ref, boolean staged) {
    ObjectNode tableJson = OBJECT_MAPPER.valueToTree(table);
    ObjectNode properties = (ObjectNode) tableJson.path("properties");
    if (properties == null) {
      properties = tableJson.putObject("properties");
    }
    properties.put(staged ? PROPERTY_STAGED_REF : PROPERTY_LOAD_REF, ref.getName());
    properties.put(staged ? PROPERTY_STAGED_COMMIT : PROPERTY_LOAD_COMMIT, ref.getHash());
    properties.remove(staged ? PROPERTY_LOAD_REF : PROPERTY_STAGED_REF);
    properties.remove(staged ? PROPERTY_LOAD_COMMIT : PROPERTY_STAGED_COMMIT);

    if (!staged) {
      // Properties from NessieCatalog in Apache Iceberg
      properties.put(PROPERTY_COMMIT_ID, ref.getHash());
    } else {
      properties.remove(PROPERTY_COMMIT_ID);
    }
    // To prevent accidental deletion of files that are still referenced by other branches/tags,
    // setting GC_ENABLED to false. So that all Iceberg's gc operations like expire_snapshots,
    // remove_orphan_files, drop_table with purge will fail with an error.
    // Nessie provides a reference aware GC functionality for the expired/unreferenced files.
    properties.put(TableProperties.GC_ENABLED, "false");

    table = jsonToTableMetadata(table.metadataFileLocation(), tableJson);

    Map<String, String> tableConfig = emptyMap();
    return LoadTableResponse.builder().addAllConfig(tableConfig).withTableMetadata(table).build();
  }

  private ContentResponse fetchIcebergTable(TableRef tableRef) throws NessieNotFoundException {
    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();
    ContentResponse content =
        api.getContent()
            .refName(tableRef.referenceName())
            .hashOnRef(tableRef.referenceHash())
            .getSingle(tableRef.contentKey());
    checkArgument(
        content.getContent().getType().equals(Content.Type.ICEBERG_TABLE),
        "Table is not an Iceberg table, it is of type %s",
        content.getContent().getType());
    return content;
  }
}

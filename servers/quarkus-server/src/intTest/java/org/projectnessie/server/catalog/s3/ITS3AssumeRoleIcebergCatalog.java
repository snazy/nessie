/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.server.catalog.s3;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.projectnessie.minio.MinioContainer;
import org.projectnessie.server.catalog.Catalogs;
import org.projectnessie.server.catalog.MinioTestResourceLifecycleManager;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@QuarkusTestResource(
    restrictToAnnotatedClass = true,
    value = MinioTestResourceLifecycleManager.class)
@QuarkusIntegrationTest
@TestProfile(ITS3AssumeRoleIcebergCatalog.Profile.class)
public class ITS3AssumeRoleIcebergCatalog {

  private static final String IAM_POLICY =
      """
      { "Version":"2012-10-17",
        "Statement": [
          {"Sid":"A1", "Effect":"Allow", "Action":"s3:*", "Resource":"arn:aws:s3:::*"},
          {"Sid":"D1", "Effect":"Deny", "Action":"s3:*", "Resource":"arn:aws:s3:::*/blockedNamespace/*"}
         ]
      }
      """;

  @SuppressWarnings("unused")
  // Injected by MinioTestResourceLifecycleManager
  private MinioContainer minio;

  private static final Catalogs CATALOGS = new Catalogs();

  RESTCatalog catalog() {
    return CATALOGS.getCatalog(
        Map.of(
            AwsClientProperties.CLIENT_REGION,
            MinioTestResourceLifecycleManager.TEST_REGION,
            CatalogProperties.WAREHOUSE_LOCATION,
            minio.s3BucketUri("").toString()));
  }

  @AfterAll
  static void closeRestCatalog() throws Exception {
    CATALOGS.close();
  }

  @Test
  void testCreateTable() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    Namespace ns = Namespace.of("allowedNamespace");
    catalog.createNamespace(ns);

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    // Create a table exercises assume role flows.
    catalog.createTable(TableIdentifier.of(ns, "table1"), schema);
  }

  @Test
  void testCreateTableForbidden() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    Namespace ns = Namespace.of("blockedNamespace");
    catalog.createNamespace(ns);

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    // Attempts to create files blocked by the session IAM policy break the createTable() call
    assertThatThrownBy(() -> catalog.createTable(TableIdentifier.of(ns, "table1"), schema))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("S3Exception: Access Denied")
        // make sure the error comes from the Catalog Server
        .hasStackTraceContaining("org.apache.iceberg.rest.RESTClient");
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("nessie.catalog.service.s3.default-options.session-iam-policy", IAM_POLICY)
          .put(
              "nessie.catalog.service.s3.default-options.assume-role",
              "test-role") // Note: unused by Minio
          .put(
              "nessie.catalog.service.s3.default-options.external-id",
              "test-external-id") // Note: unused by Minio
          .build();
    }
  }
}

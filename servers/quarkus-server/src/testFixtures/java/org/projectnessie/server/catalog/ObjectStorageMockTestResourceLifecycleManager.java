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
package org.projectnessie.server.catalog;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.objectstoragemock.ObjectStorageMock.MockServer;
import org.projectnessie.objectstoragemock.sts.AssumeRoleHandler;
import org.projectnessie.objectstoragemock.sts.AssumeRoleResult;

public class ObjectStorageMockTestResourceLifecycleManager
    implements QuarkusTestResourceLifecycleManager {

  public static final String BUCKET = "bucket1";

  public static String bucketWarehouseLocation(String scheme) {
    return String.format("%s://%s/warehouse", scheme, BUCKET);
  }

  public static final String S3_WAREHOUSE_LOCATION = bucketWarehouseLocation("s3");
  public static final String S3A_WAREHOUSE_LOCATION = bucketWarehouseLocation("s3a");
  public static final String S3N_WAREHOUSE_LOCATION = bucketWarehouseLocation("s3n");
  public static final String GCS_WAREHOUSE_LOCATION = bucketWarehouseLocation("gs");
  public static final String ADLS_WAREHOUSE_LOCATION =
      "abfs://" + BUCKET + "@account.dfs.core.windows.net/warehouse";

  public static final String INIT_ADDRESS =
      "ObjectStorageMockTestResourceLifecycleManager.initAddress";

  private final AssumeRoleHandlerHolder assumeRoleHandler = new AssumeRoleHandlerHolder();

  private HeapStorageBucket heapStorageBucket;
  private MockServer server;

  @Override
  public Map<String, String> start() {

    heapStorageBucket = HeapStorageBucket.newHeapStorageBucket();
    server =
        ObjectStorageMock.builder()
            .initAddress("localhost")
            .putBuckets(BUCKET, heapStorageBucket.bucket())
            .assumeRoleHandler(assumeRoleHandler)
            .build()
            .start();

    String s3Endpoint = server.getS3BaseUri().toString();
    String gcsEndpoint = server.getGcsBaseUri().toString();
    String adlsEndpoint = server.getAdlsGen2BaseUri().resolve(BUCKET).toString();

    return ImmutableMap.<String, String>builder()
        // S3
        .put(
            "nessie.catalog.service.s3.default-options.sts-endpoint",
            server.getStsEndpointURI().toString())
        .put("nessie.catalog.service.s3.buckets." + BUCKET + ".endpoint", s3Endpoint)
        .put("nessie.catalog.service.s3.buckets." + BUCKET + ".region", "us-east-1")
        .put("nessie.catalog.service.s3.buckets." + BUCKET + ".path-style-access", "true")
        .put("nessie.catalog.service.s3.buckets." + BUCKET + ".access-key.name", "accessKey")
        .put("nessie.catalog.service.s3.buckets." + BUCKET + ".access-key.secret", "secretKey")
        // GCS
        .put("nessie.catalog.service.gcs.buckets." + BUCKET + ".host", gcsEndpoint)
        .put("nessie.catalog.service.gcs.buckets." + BUCKET + ".project-id", "my-project")
        .put("nessie.catalog.service.gcs.buckets." + BUCKET + ".auth-type", "none")
        // ADLS
        .put("nessie.catalog.service.adls.file-systems." + BUCKET + ".endpoint", adlsEndpoint)
        .put("nessie.catalog.service.adls.file-systems." + BUCKET + ".sas-token", "token")
        .build();
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(
        heapStorageBucket, new TestInjector.MatchesType(HeapStorageBucket.class));

    testInjector.injectIntoFields(
        assumeRoleHandler, new TestInjector.MatchesType(AssumeRoleHandlerHolder.class));
  }

  @Override
  public void stop() {
    if (server != null) {
      try {
        server.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        server = null;
      }
    }
  }

  public static final class AssumeRoleHandlerHolder implements AssumeRoleHandler {
    private final AtomicReference<AssumeRoleHandler> handler = new AtomicReference<>();

    public void set(AssumeRoleHandler handler) {
      this.handler.set(handler);
    }

    @Override
    public AssumeRoleResult assumeRole(
        String action,
        String version,
        String roleArn,
        String roleSessionName,
        String policy,
        Integer durationSeconds,
        String externalId,
        String serialNumber) {
      return handler
          .get()
          .assumeRole(
              action,
              version,
              roleArn,
              roleSessionName,
              policy,
              durationSeconds,
              externalId,
              serialNumber);
    }
  }
}

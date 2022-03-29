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
package org.projectnessie.integtests.iceberg;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

public interface BaseSparkConfigs extends BaseSpark {
  String NESSIE_URI = System.getProperty("quarkus.http.url");
  String SPARK_MASTER_URL = System.getProperty("spark.master.url");
  String NON_NESSIE_CATALOG = "invalid_hive";

  AtomicReference<SparkSession> SPARK_SESSION = new AtomicReference<>();
  AtomicReference<NessieApiV1> NESSIE_API = new AtomicReference<>();

  @BeforeAll
  static void setupApi() throws Exception {
    NessieApiV1 api = HttpClientBuilder.builder().withUri(NESSIE_URI).build(NessieApiV1.class);
    NESSIE_API.set(api);
    api.getDefaultBranch();
  }

  @BeforeEach
  default void setupSparkConf(@TempDir File tempDir) {
    SparkConf conf = new SparkConf();

    System.err.println(NESSIE_URI);

    Map<String, String> nessieParams =
        ImmutableMap.of("ref", "main", "uri", NESSIE_URI, "warehouse", tempDir.toURI().toString());

    nessieParams.forEach(
        (k, v) -> {
          conf.set(String.format("spark.sql.catalog.nessie.%s", k), v);
          conf.set(String.format("spark.sql.catalog.spark_catalog.%s", k), v);
        });

    conf.set(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .set("spark.testing", "true")
        .set("spark.sql.shuffle.partitions", "4")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog");

    updateSparkConf(conf);

    // the following catalog is only added to test a check in the nessie spark extensions
    conf.set(
            String.format("spark.sql.catalog.%s", NON_NESSIE_CATALOG),
            "org.apache.iceberg.spark.SparkCatalog")
        .set(
            String.format("spark.sql.catalog.%s.catalog-impl", NON_NESSIE_CATALOG),
            "org.apache.iceberg.hive.HiveCatalog");

    updateSparkConf(conf);

    SparkSession spark = SparkSession.builder().master(SPARK_MASTER_URL).config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    SPARK_SESSION.set(spark);
  }

  default void updateSparkConf(SparkConf conf) {}

  @AfterAll
  static void stopSpark() {
    SparkSession spark = SPARK_SESSION.getAndSet(null);
    if (spark != null) {
      spark.stop();
    }
  }

  @AfterAll
  static void closeApi() {
    NessieApiV1 api = NESSIE_API.getAndSet(null);
    if (api != null) {
      api.close();
    }
  }

  @AfterEach
  default void resetNessie() throws Exception {
    String defaultBranchName = api().getDefaultBranch().getName();
    for (Reference ref : api().getAllReferences().get().getReferences()) {
      if (ref instanceof Branch) {
        api().deleteBranch().branchName(ref.getName()).hash(ref.getHash()).delete();
      }
      if (ref instanceof Tag) {
        api().deleteTag().tagName(ref.getName()).hash(ref.getHash()).delete();
      }
    }
    api().createReference().reference(Branch.of(defaultBranchName, null)).create();
  }

  @Override
  default NessieApiV1 api() {
    return NESSIE_API.get();
  }

  @Override
  default SparkSession spark() {
    return SPARK_SESSION.get();
  }
}

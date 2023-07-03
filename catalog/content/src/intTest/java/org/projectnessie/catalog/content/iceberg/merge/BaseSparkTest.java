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

import static org.apache.spark.sql.internal.SQLConf.PARTITION_OVERWRITE_MODE;
import static org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS;
import static org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS;
import static org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.catalog.Catalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.catalog.content.iceberg.LocalFileIO;

public abstract class BaseSparkTest {
  protected static SparkConf conf = new SparkConf();

  protected static SparkSession spark;

  protected static final String CATALOG_NAME = "testing";

  @TempDir protected Path sparkWarehouse;

  @TempDir protected Path icebergWarehouse;

  @BeforeEach
  public void setupSparkPerTest() {
    String forCatalog = "spark.sql.catalog." + CATALOG_NAME;
    String extensions = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";
    conf.set(PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .set("spark.testing", "true")
        .set("spark.ui.enabled", "false")
        .set(WAREHOUSE_PATH().key(), sparkWarehouse.toUri().toString())
        .set(SHUFFLE_PARTITIONS().key(), "4")
        .set(SPARK_SESSION_EXTENSIONS().key(), extensions)
        .set(forCatalog, "org.apache.iceberg.spark.SparkCatalog")
        .set(forCatalog + ".warehouse", icebergWarehouse.toUri().toString())
        .set(forCatalog + ".type", "hadoop")
        .set(forCatalog + ".io-impl", LocalFileIO.class.getName())
        .set(forCatalog + ".cache-enabled", "false");

    spark = SparkSession.builder().master("local[2]").config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  protected static List<Object[]> execSql(String sql) {
    List<Row> rows = spark.sql(sql).collectAsList();
    if (rows.isEmpty()) {
      return ImmutableList.of();
    }

    return rows.stream().map(BaseSparkTest::toJava).collect(Collectors.toList());
  }

  protected static Object[] toJava(Row row) {
    return IntStream.range(0, row.size())
        .mapToObj(
            pos -> {
              if (row.isNullAt(pos)) {
                return null;
              }

              Object value = row.get(pos);
              if (value instanceof Row) {
                return toJava((Row) value);
              } else if (value instanceof scala.collection.Seq) {
                return row.getList(pos);
              } else if (value instanceof scala.collection.Map) {
                return row.getJavaMap(pos);
              } else {
                return value;
              }
            })
        .toArray(Object[]::new);
  }

  protected static Catalog icebergCatalog() {
    @SuppressWarnings("resource")
    CatalogPlugin catalogImpl =
        SparkSession.active().sessionState().catalogManager().catalog(CATALOG_NAME);
    return icebergCatalogFromSparkCatalog(catalogImpl);
  }

  private static Catalog icebergCatalogFromSparkCatalog(CatalogPlugin catalogImpl) {
    try {
      // `catalogImpl` is an `org.apache.iceberg.spark.SparkCatalog`...
      try {
        // ... and most(!!) implementations of `o.a.i.s.SparkCatalog` have a
        // `public Catalog icebergCatalog()` function...
        return (Catalog)
            catalogImpl.getClass().getDeclaredMethod("icebergCatalog").invoke(catalogImpl);
      } catch (NoSuchMethodException methor) {
        // ... but not *ALL* have that function, so we need to refer to the
        // field in these cases. :facepalm:
        Field icebergCatalogField = catalogImpl.getClass().getDeclaredField("icebergCatalog");
        icebergCatalogField.setAccessible(true);
        return (Catalog) icebergCatalogField.get(catalogImpl);
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

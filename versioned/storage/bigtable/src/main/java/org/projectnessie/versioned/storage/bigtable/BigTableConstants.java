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
package org.projectnessie.versioned.storage.bigtable;

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;
import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.protobuf.ByteString;

final class BigTableConstants {

  static final String TABLE_REFS = "refs";
  static final String TABLE_OBJS = "objs";

  static final String FAMILY_REFS = "r";
  static final String FAMILY_OBJS = "o";

  static final ByteString QUALIFIER_OBJ_TYPE = ByteString.copyFromUtf8("t");
  static final ByteString QUALIFIER_OBJS = ByteString.copyFromUtf8("o");
  static final ByteString QUALIFIER_REFS = ByteString.copyFromUtf8("r");

  public static final Filters.Filter FILTER_LIMIT_1 = FILTERS.limit().cellsPerColumn(1);
  public static final Filters.Filter FILTER_FAMILY_REFS = FILTERS.family().exactMatch(FAMILY_REFS);
  public static final Filters.Filter FILTER_QUALIFIER_REFS =
      FILTERS.qualifier().exactMatch(QUALIFIER_REFS);
  public static final Filters.Filter FILTER_FAMILY_OBJS = FILTERS.family().exactMatch(FAMILY_OBJS);
  public static final Filters.Filter FILTER_QUALIFIER_OBJS =
      FILTERS.qualifier().exactMatch(QUALIFIER_OBJS);

  static final GCRules.VersionRule GCRULE_MAX_VERSIONS_1 = GCRULES.maxVersions(1);

  static final int MAX_PARALLEL_READS = 5;
  static final int MAX_BULK_READS = 100;
  static final int MAX_BULK_MUTATIONS = 1000;

  static final long READ_TIMEOUT_MILLIS = 5000L;

  private BigTableConstants() {}
}

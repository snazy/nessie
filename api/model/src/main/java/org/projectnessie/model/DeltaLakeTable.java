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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.immutables.value.Value;
import org.projectnessie.jackson.protobuf.api.ProtoType;

@Value.Immutable
@JsonSerialize(as = ImmutableDeltaLakeTable.class)
@JsonDeserialize(as = ImmutableDeltaLakeTable.class)
@JsonTypeName("DELTA_LAKE_TABLE")
@ProtoType(protoType = "nessie.model.DeltaLakeTable")
public abstract class DeltaLakeTable extends Content {

  @NotNull
  @jakarta.validation.constraints.NotNull
  public abstract List<String> getMetadataLocationHistory();

  @NotNull
  @jakarta.validation.constraints.NotNull
  public abstract List<String> getCheckpointLocationHistory();

  @Nullable
  @jakarta.annotation.Nullable
  public abstract String getLastCheckpoint();

  @Override
  public Type getType() {
    return Type.DELTA_LAKE_TABLE;
  }

  @Override
  public abstract DeltaLakeTable withId(String id);
}

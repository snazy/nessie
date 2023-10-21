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
package org.projectnessie.jackson.protobuf;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.immutables.value.Value;
import org.projectnessie.nessie.relocated.protobuf.Descriptors;

@Value.Immutable
@Value.Style(allParameters = true)
abstract class ProtoMessageType {

  abstract Map<String, Descriptors.FieldDescriptor> protoFields();

  abstract Descriptors.Descriptor descriptor();

  abstract Class<?> protoValueClass();

  abstract Class<?> protoBuilderClass();

  abstract Supplier<?> protoBuilderSupplier();

  abstract Function<Object, ?> protoBuilderBuild();

  @FunctionalInterface
  interface SetField {
    void setField(Object builder, Object fieldDescriptor, Object value);
  }

  abstract SetField protoBuilderSetField();
}

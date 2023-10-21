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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.projectnessie.jackson.protobuf.api.ProtoType;
import org.projectnessie.jackson.protobuf.api.ProtobufGenerator;
import org.projectnessie.nessie.relocated.protobuf.DescriptorProtos;
import org.projectnessie.nessie.relocated.protobuf.Descriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ProtobufMapper extends ObjectMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufMapper.class);

  private final Map<String, Descriptors.FileDescriptor> fileDescriptorMap = new HashMap<>();
  private final Map<String, Descriptors.Descriptor> descriptorMap = new HashMap<>();
  private final Map<String, Descriptors.EnumDescriptor> enumDescriptorMap = new HashMap<>();
  private final Map<String, String> protoRelocations = new HashMap<>();

  public ProtobufMapper() {
    this(new ProtobufFactory());
  }

  public ProtobufMapper(JsonFactory jf) {
    super(requireProtobufFactory(jf));
    registerModule(new ProtobufModule());
  }

  private static JsonFactory requireProtobufFactory(JsonFactory jf) {
    checkArgument(
        jf instanceof ProtobufFactory,
        "Must pass an instance of %s to %s.",
        ProtobufFactory.class.getName(),
        ProtobufMapper.class.getName());
    return jf;
  }

  ProtobufMapper(ObjectMapper src) {
    super(src);
  }

  public ProtobufMapper registerRelocation(String fromPackage, String toPackage) {
    protoRelocations.put(fromPackage, toPackage);
    return this;
  }

  Descriptors.Descriptor protoDescriptor(String fullName) {
    return descriptorMap.get(fullName);
  }

  Descriptors.EnumDescriptor protoEnumDescriptor(String fullName) {
    return enumDescriptorMap.get(fullName);
  }

  private final Map<JavaType, ProtoBeanType> protoBeanTypeMap = new ConcurrentHashMap<>();
  private final Map<String, ProtoMessageType> protoMessageTypeMap = new ConcurrentHashMap<>();

  ProtoBeanType protoBeanType(JavaType javaType) {
    return protoBeanTypeMap.computeIfAbsent(
        javaType,
        t -> {
          try {
            BeanDescription beanDescription = _serializationConfig.introspect(t);
            ProtoType protoType = beanDescription.getClassAnnotations().get(ProtoType.class);
            if (protoType == null) {
              return null;
            }

            return buildProtoBeanType(beanDescription, protoType);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  ProtoMessageType protoMessageType(Descriptors.Descriptor protoDescriptor) {
    return protoMessageTypeMap.computeIfAbsent(
        protoDescriptor.getFullName(),
        t -> {
          try {
            return buildProtoMessageType(protoDescriptor.getFullName(), protoDescriptor);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  ProtoMessageType protoMessageType(String protoFullName) {
    return protoMessageTypeMap.computeIfAbsent(
        protoFullName,
        t -> {
          try {
            return buildProtoMessageType(protoFullName, null);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  private ProtoBeanType buildProtoBeanType(BeanDescription beanDescription, ProtoType protoType) {
    LOGGER.trace(
        "Building proto-bean information for Jackson type {} to proto type {}",
        beanDescription.getType(),
        protoType.protoType());

    ProtoMessageType protoMessageType = protoMessageType(protoType.protoType());

    Map<String, ProtoMessageType> resolvedMessageTypes = new HashMap<>();
    Map<String, Descriptors.Descriptor> messageTypes = new HashMap<>();
    Deque<String> pendingProtoNames = new ArrayDeque<>();
    pendingProtoNames.add(protoType.protoType());
    while (true) {
      String protoName = pendingProtoNames.poll();
      if (protoName == null) {
        break;
      }

      if (resolvedMessageTypes.containsKey(protoName)) {
        continue;
      }

      LOGGER.trace("Resolving dependant message type {} for {}", protoName, protoType.protoType());
      ProtoMessageType messageType =
          messageTypes.containsKey(protoName)
              ? protoMessageType(messageTypes.get(protoName))
              : protoMessageType(protoName);
      resolvedMessageTypes.put(protoName, messageType);

      for (Descriptors.FieldDescriptor field : messageType.descriptor().getFields()) {
        if (field.isMapField()) {
          List<Descriptors.FieldDescriptor> mapFields = field.getMessageType().getFields();
          // key
          Descriptors.FieldDescriptor fieldDescriptor = mapFields.get(0);
          if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            Descriptors.Descriptor desc = fieldDescriptor.getMessageType();
            if (!resolvedMessageTypes.containsKey(desc.getFullName())) {
              messageTypes.put(desc.getFullName(), desc);
              pendingProtoNames.add(desc.getFullName());
            }
          }
          // value
          fieldDescriptor = mapFields.get(1);
          if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            Descriptors.Descriptor desc = fieldDescriptor.getMessageType();
            if (!resolvedMessageTypes.containsKey(desc.getFullName())) {
              messageTypes.put(desc.getFullName(), desc);
              pendingProtoNames.add(desc.getFullName());
            }
          }
        } else {
          if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            Descriptors.Descriptor desc = field.getMessageType();
            if (!resolvedMessageTypes.containsKey(desc.getFullName())) {
              messageTypes.put(desc.getFullName(), desc);
              pendingProtoNames.add(desc.getFullName());
            }
          }
        }
      }
    }

    return ImmutableProtoBeanType.of(
        beanDescription,
        beanDescription.findProperties().stream()
            .collect(Collectors.toMap(BeanPropertyDefinition::getName, Function.identity())),
        protoMessageType);
  }

  private ProtoMessageType buildProtoMessageType(
      String protoFullName, Descriptors.Descriptor descriptor) throws Exception {
    LOGGER.trace("Building proto-message information for proto type {}", protoFullName);

    if (descriptor == null) {
      descriptor = protoDescriptor(protoFullName);
    }
    checkState(descriptor != null, "No protobuf descriptor for message type %s", protoFullName);

    String protoValueClassName;
    if (descriptor.getOptions().getMapEntry()) {
      protoValueClassName = protoJavaClass("com.google.protobuf.", "MapEntry", protoRelocations);
    } else {
      protoValueClassName =
          protoJavaClass(descriptor.getName(), descriptor.getFile().getOptions(), protoRelocations);
    }

    Class<?> protoValueClass = Class.forName(protoValueClassName);
    Class<?> protoBuilderClass = Class.forName(protoValueClassName + "$Builder");
    Method newBuilderMethod = protoValueClass.getDeclaredMethod("newBuilder");
    Method builderBuildMethod = protoBuilderClass.getDeclaredMethod("build");
    Method builderSetFieldMethod =
        Arrays.stream(protoBuilderClass.getDeclaredMethods())
            .filter(m -> m.getName().equals("setField"))
            .filter(
                m -> {
                  Class<?>[] parameterTypes = m.getParameterTypes();
                  return parameterTypes.length == 2
                      && parameterTypes[0].getSimpleName().equals("FieldDescriptor")
                      && parameterTypes[1] == Object.class;
                })
            .findFirst()
            .orElseThrow(IllegalStateException::new);

    Supplier<?> builderSupplier =
        () -> {
          try {
            return newBuilderMethod.invoke(null);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };
    Function<Object, ?> builderBuild =
        builder -> {
          try {
            return builderBuildMethod.invoke(builder);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };
    ProtoMessageType.SetField builderSetField =
        (builder, field, value) -> {
          try {
            builderSetFieldMethod.invoke(builder, field, value);
          } catch (Exception e) {
            throw new RuntimeException("Failed to set field " + field, e);
          }
        };

    return ImmutableProtoMessageType.of(
        descriptor.getFields().stream()
            .collect(
                Collectors.toMap(Descriptors.FieldDescriptor::getJsonName, Function.identity())),
        descriptor,
        protoValueClass,
        protoBuilderClass,
        builderSupplier,
        builderBuild,
        builderSetField);
  }

  private static String protoJavaClass(
      String name, DescriptorProtos.FileOptions options, Map<String, String> relocations) {
    String pkg = options.getJavaPackage() + '.';
    String cls;
    if (options.getJavaMultipleFiles()) {
      cls = name;
    } else {
      cls = options.getJavaOuterClassname() + '$' + name;
    }
    return protoJavaClass(pkg, cls, relocations);
  }

  private static String protoJavaClass(String pkg, String cls, Map<String, String> relocations) {
    return relocations.getOrDefault(pkg, pkg) + cls;
  }

  public ProtobufMapper registerFileDescriptor(
      byte[] fileDescriptorProto, List<byte[]> dependencies) throws Exception {
    Descriptors.FileDescriptor[] deps = new Descriptors.FileDescriptor[dependencies.size()];
    for (int i = 0; i < dependencies.size(); i++) {
      deps[i] =
          Descriptors.FileDescriptor.buildFrom(
              DescriptorProtos.FileDescriptorProto.parseFrom(dependencies.get(i)),
              new Descriptors.FileDescriptor[0],
              true);
    }
    registerFileDescriptor(
        Descriptors.FileDescriptor.buildFrom(
            DescriptorProtos.FileDescriptorProto.parseFrom(fileDescriptorProto), deps));
    return this;
  }

  public ProtobufMapper registerFileDescriptor(Descriptors.FileDescriptor fileDescriptor) {
    for (Descriptors.FileDescriptor descriptor : collectAllFileDescriptors(fileDescriptor)) {
      fileDescriptorMap.put(descriptor.getFullName(), descriptor);
      for (Descriptors.Descriptor messageType : descriptor.getMessageTypes()) {
        descriptorMap.put(messageType.getFullName(), messageType);
      }
      for (Descriptors.EnumDescriptor enumType : descriptor.getEnumTypes()) {
        enumDescriptorMap.put(enumType.getFullName(), enumType);
      }
    }
    return this;
  }

  private Collection<Descriptors.FileDescriptor> collectAllFileDescriptors(
      Descriptors.FileDescriptor initial) {
    Deque<Descriptors.FileDescriptor> pending = new ArrayDeque<>();
    pending.add(initial);

    Map<String, Descriptors.FileDescriptor> allFileDescriptors = new HashMap<>();

    while (true) {
      Descriptors.FileDescriptor fileDescriptor = pending.poll();
      if (fileDescriptor == null) {
        break;
      }

      allFileDescriptors.put(fileDescriptor.getFullName(), fileDescriptor);

      for (Descriptors.FileDescriptor dependency : fileDescriptor.getDependencies()) {
        if (!allFileDescriptors.containsKey(dependency.getFullName())) {
          pending.add(dependency);
        }
      }
      for (Descriptors.FileDescriptor publicDependency : fileDescriptor.getPublicDependencies()) {
        if (!allFileDescriptors.containsKey(publicDependency.getFullName())) {
          pending.add(publicDependency);
        }
      }
    }

    return allFileDescriptors.values();
  }

  @Override
  public ProtobufMapper copy() {
    _checkInvalidCopy(ProtobufMapper.class);
    return new ProtobufMapper(this);
  }

  public <T> T writeValueAsProtoObject(Object value, Class<T> protoType) throws IOException {
    SerializationConfig cfg = getSerializationConfig();
    ProtobufGenerator g = ((ProtobufFactory) _jsonFactory).createProtoObjectGenerator();
    JsonGenerator jg = (JsonGenerator) g;
    cfg.initialize(jg);
    _serializerProvider(cfg).serializeValue(jg, value);
    return g.getRootValue(protoType);
  }

  public <T> T readValueFromProtoObject(Object protoObject, Class<T> valueType) throws IOException {
    ProtobufFactory protobufFactory = (ProtobufFactory) _jsonFactory;
    @SuppressWarnings({"UnnecessaryLocalVariable", "unchecked"})
    T r =
        (T)
            _readMapAndClose(
                protobufFactory._createParserForProto(protoObject),
                _typeFactory.constructType(valueType));
    return r;
  }

  @Override
  protected JsonToken _initForReading(JsonParser p, JavaType targetType) throws IOException {
    ProtobufParserImpl protobufParser = (ProtobufParserImpl) p;
    protobufParser.rootTargetType(targetType);
    return super._initForReading(p, targetType);
  }

  @Override
  protected ObjectReader _newReader(DeserializationConfig config) {
    return super._newReader(config);
  }

  @Override
  protected ObjectReader _newReader(
      DeserializationConfig config,
      JavaType valueType,
      Object valueToUpdate,
      FormatSchema schema,
      InjectableValues injectableValues) {
    return super._newReader(config, valueType, valueToUpdate, schema, injectableValues);
  }

  @Override
  protected ObjectWriter _newWriter(SerializationConfig config) {
    return new ProtobufWriter(this, config);
  }

  @Override
  protected ObjectWriter _newWriter(SerializationConfig config, FormatSchema schema) {
    return new ProtobufWriter(this, config, schema);
  }

  @Override
  protected ObjectWriter _newWriter(
      SerializationConfig config, JavaType rootType, PrettyPrinter pp) {
    return new ProtobufWriter(this, config, rootType, pp);
  }

  @Override
  public ProtobufWriter writerFor(Class<?> rootType) {
    return (ProtobufWriter) super.writerFor(rootType);
  }

  @Override
  public ProtobufWriter writerFor(TypeReference<?> rootType) {
    return (ProtobufWriter) super.writerFor(rootType);
  }

  @Override
  public ProtobufWriter writerFor(JavaType rootType) {
    return (ProtobufWriter) super.writerFor(rootType);
  }
}

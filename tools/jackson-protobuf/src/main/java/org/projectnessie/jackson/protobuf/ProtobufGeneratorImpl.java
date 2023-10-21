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

import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.core.Base64Variant;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.base.GeneratorBase;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.type.WritableTypeId;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import org.projectnessie.jackson.protobuf.api.ProtobufGenerator;
import org.projectnessie.nessie.relocated.protobuf.DescriptorProtos;
import org.projectnessie.nessie.relocated.protobuf.Descriptors;
import org.projectnessie.nessie.relocated.protobuf.GeneratedMessage;
import org.projectnessie.nessie.relocated.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ProtobufGeneratorImpl extends GeneratorBase implements ProtobufGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufGeneratorImpl.class);

  private final ProtobufMapper mapper;
  private WritableTypeId typeIdDef;
  private JavaType rootType;

  private Object rootValue;

  ProtobufGeneratorImpl(
      ProtobufMapper mapper,
      ObjectCodec objectCodec,
      int generatorFeatures,
      OutputStream outputStream) {
    super(generatorFeatures, objectCodec, (IOContext) null);
    this.mapper = mapper;
    pushContext(new RootGenContext(null, outputStream));
  }

  void setRootType(JavaType rootType) {
    this.rootType = rootType;
  }

  private final Deque<GenContext> genContextStack = new ArrayDeque<>();

  private GenContext currentContext() {
    return genContextStack.getLast();
  }

  private <T extends GenContext> void pushContext(T context) {
    genContextStack.addLast(context);
  }

  private <T extends GenContext> T popContext(Class<T> expected) {
    GenContext context = genContextStack.removeLast();
    checkState(
        expected.isAssignableFrom(context.getClass()),
        "Expected current serialization context to be an instance of %s, but is %s",
        expected.getName(),
        context.getClass().getName());
    return expected.cast(context);
  }

  abstract static class GenContext {
    final GenContext parent;
    JavaType jacksonType;
    Object value;

    GenContext(GenContext parent) {
      this.parent = parent;
    }

    abstract void currentObject(Object v);

    abstract Object finish();

    abstract void pushValue(Object value);

    abstract void pushString(String s);

    abstract void pushBinary(byte[] bytes, int offset, int len);

    abstract void pushNumber(String value);

    abstract void pushNumber(long value);

    abstract void pushNumber(int value);

    abstract void pushNumber(float value);

    abstract void pushNumber(double value);

    abstract void pushNumber(BigInteger value);

    abstract void pushNumber(BigDecimal value);

    abstract void pushBoolean(boolean value);

    abstract void setFieldName(String fieldName);
  }

  @Override
  public <T> T getRootValue(Class<T> valueType) {
    return valueType.cast(rootValue);
  }

  final class RootGenContext extends GenContext {
    private final OutputStream outputStream;

    RootGenContext(GenContext parent, OutputStream outputStream) {
      super(parent);
      this.outputStream = outputStream;
    }

    @Override
    void pushValue(Object value) {
      rootValue = value;
      if (outputStream != null) {
        try {
          ((Message) value).writeTo(outputStream);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    Object finish() {
      throw new UnsupportedOperationException();
    }

    @Override
    void currentObject(Object v) {
      throw new UnsupportedOperationException();
    }

    @Override
    void pushString(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    void pushBinary(byte[] bytes, int offset, int len) {
      throw new UnsupportedOperationException();
    }

    @Override
    void pushNumber(String value) {
      throw new UnsupportedOperationException();
    }

    @Override
    void pushNumber(long value) {
      throw new UnsupportedOperationException();
    }

    @Override
    void pushNumber(int value) {
      throw new UnsupportedOperationException();
    }

    @Override
    void pushNumber(float value) {
      throw new UnsupportedOperationException();
    }

    @Override
    void pushNumber(double value) {
      throw new UnsupportedOperationException();
    }

    @Override
    void pushNumber(BigInteger value) {
      throw new UnsupportedOperationException();
    }

    @Override
    void pushNumber(BigDecimal value) {
      throw new UnsupportedOperationException();
    }

    @Override
    void pushBoolean(boolean value) {
      throw new UnsupportedOperationException();
    }

    @Override
    void setFieldName(String fieldName) {
      throw new UnsupportedOperationException();
    }
  }

  final class ObjectGenContext extends GenContext {
    final WritableTypeId typeIdDef;
    JavaType convertToType;
    Object builderInstance;
    String fieldName;
    BeanPropertyDefinition property;
    Descriptors.FieldDescriptor protoField;
    ProtoBeanType protoBeanType;
    ProtoMessageType protoMessageType;

    ObjectGenContext(GenContext parent, WritableTypeId typeIdDef, JavaType convertToType) {
      super(parent);
      this.typeIdDef = typeIdDef;
      this.convertToType = convertToType;
    }

    @Override
    void currentObject(Object v) {
      value = v;
      jacksonType = mapper.constructType(v.getClass());
      protoBeanType = mapper.protoBeanType(jacksonType);
      protoMessageType = protoBeanType.messageType();
      builderInstance = protoMessageType.protoBuilderSupplier().get();
    }

    @Override
    Object finish() {
      Object value = protoMessageType.protoBuilderBuild().apply(builderInstance);

      if (typeIdDef != null) {
        // TODO Do something with the Jackson type ID
        LOGGER.trace(
            "Type ID for value to push is {}, property {}, value type {}",
            typeIdDef.id,
            typeIdDef.asProperty,
            typeIdDef.forValueType);

        if (convertToType != null) {
          // TODO memoize the calculations below

          ProtoBeanType protoBeanType = mapper.protoBeanType(convertToType);
          ProtoMessageType protoMessageType = protoBeanType.messageType();
          Descriptors.Descriptor descriptor = protoMessageType.descriptor();
          // List<Descriptors.OneofDescriptor> realOneofs = descriptor.getRealOneofs();
          List<Descriptors.OneofDescriptor> oneofs = descriptor.getOneofs();
          Descriptors.OneofDescriptor oneof =
              oneofs.stream()
                  .filter(o -> o.getName().equals(typeIdDef.asProperty))
                  .findFirst()
                  .orElse(null);

          if (oneof != null) {
            Descriptors.FileDescriptor file = descriptor.getFile();
            Descriptors.FieldDescriptor ext = file.findExtensionByName("jackson_type_id");

            GeneratedMessage.GeneratedExtension<DescriptorProtos.FieldOptions, String>
                jacksonTypeIdExtension =
                    GeneratedMessage.newFileScopedGeneratedExtension(String.class, null);
            jacksonTypeIdExtension.internalInit(ext);

            Descriptors.FieldDescriptor field =
                oneof.getFields().stream()
                    .filter(
                        f -> {
                          DescriptorProtos.FieldOptions fieldOpts = f.getOptions();
                          String typeIdExt = fieldOpts.getExtension(jacksonTypeIdExtension);
                          return typeIdDef.id.equals(typeIdExt);
                        })
                    .findFirst()
                    .orElse(null);

            Object builderInstance = protoMessageType.protoBuilderSupplier().get();
            protoMessageType.protoBuilderSetField().setField(builderInstance, field, value);
            value = protoMessageType.protoBuilderBuild().apply(builderInstance);
          }
        }
      }

      parent.pushValue(value);
      return value;
    }

    @Override
    void pushValue(Object value) {
      LOGGER.trace("Value for {} --> {}", fieldName, value);
      if (protoField != null) {
        protoMessageType.protoBuilderSetField().setField(builderInstance, protoField, value);
      }
      clearField();
    }

    @Override
    void pushString(String value) {
      LOGGER.trace("Value for {} --> {}", fieldName, value);
      if (protoField != null) {
        protoMessageType.protoBuilderSetField().setField(builderInstance, protoField, value);
      }
      clearField();
    }

    @Override
    void pushBinary(byte[] bytes, int offset, int len) {
      LOGGER.trace("Value for {} --> byte[{}]", fieldName, len);
      clearField();
    }

    @Override
    void pushNumber(String value) {
      LOGGER.trace("Value for {} --> {}", fieldName, value);
      clearField();
    }

    @Override
    void pushNumber(long value) {
      LOGGER.trace("Value for {} --> {}", fieldName, value);
      if (protoField != null) {
        protoMessageType.protoBuilderSetField().setField(builderInstance, protoField, value);
      }
      clearField();
    }

    @Override
    void pushNumber(int value) {
      LOGGER.trace("Value for {} --> {}", fieldName, value);
      if (protoField != null) {
        protoMessageType.protoBuilderSetField().setField(builderInstance, protoField, value);
      }
      clearField();
    }

    @Override
    void pushNumber(float value) {
      LOGGER.trace("Value for {} --> {}", fieldName, value);
      if (protoField != null) {
        protoMessageType.protoBuilderSetField().setField(builderInstance, protoField, value);
      }
      clearField();
    }

    @Override
    void pushNumber(double value) {
      LOGGER.trace("Value for {} --> {}", fieldName, value);
      if (protoField != null) {
        protoMessageType.protoBuilderSetField().setField(builderInstance, protoField, value);
      }
      clearField();
    }

    @Override
    void pushNumber(BigInteger value) {
      LOGGER.trace("Value for {} --> {}", fieldName, value);
      clearField();
    }

    @Override
    void pushNumber(BigDecimal value) {
      LOGGER.trace("Value for {} --> {}", fieldName, value);
      clearField();
    }

    @Override
    void pushBoolean(boolean value) {
      LOGGER.trace("Value for {} --> {}", fieldName, value);
      clearField();
    }

    private void clearField() {
      fieldName = null;
      property = null;
      protoField = null;
    }

    @Override
    void setFieldName(String fieldName) {
      this.fieldName = fieldName;

      ProtoBeanType protoBeanType = mapper.protoBeanType(jacksonType);

      BeanPropertyDefinition property = protoBeanType.jacksonProperties().get(fieldName);
      this.property = property;
      if (property == null) {
        LOGGER.trace("writeFieldName(\"{}\") UNKNOWN JACKSON PROPERTY", fieldName);
        return;
      }

      protoField = protoMessageType.protoFields().get(fieldName);
      if (protoField == null) {
        LOGGER.trace("writeFieldName(\"{}\") UNKNOWN PROTO PROPERTY", fieldName);
        return;
      }

      LOGGER.trace("writeFieldName(\"{}\") - {}", fieldName, protoBeanType);
    }
  }

  final class MapGenContext extends GenContext {
    private final Descriptors.Descriptor protoEntryDescriptor;
    private final Descriptors.FieldDescriptor keyField;
    private final Descriptors.FieldDescriptor valueField;
    private final Object builderInstance;
    private final Method builderPutEntryMethod;
    private String fieldName;

    MapGenContext(GenContext parent, Descriptors.Descriptor protoEntryDescriptor) {
      super(parent);
      this.protoEntryDescriptor = protoEntryDescriptor;
      this.keyField = protoEntryDescriptor.getFields().get(0);
      this.valueField = protoEntryDescriptor.getFields().get(1);

      ObjectGenContext p = (ObjectGenContext) parent;
      String jsonFieldName = p.protoField.getJsonName();
      String builderPutEntryMethodName =
          "put" + Character.toUpperCase(jsonFieldName.charAt(0)) + jsonFieldName.substring(1);
      this.builderInstance = p.builderInstance;
      builderPutEntryMethod =
          Arrays.stream(builderInstance.getClass().getMethods())
              .filter(
                  m ->
                      m.getName().equals(builderPutEntryMethodName)
                          && m.getParameterTypes().length == 2)
              .findFirst()
              .orElseThrow(IllegalStateException::new);
    }

    @Override
    void currentObject(Object v) {
      value = v;
    }

    @Override
    Object finish() {
      return null;
    }

    @Override
    void pushValue(Object value) {
      LOGGER.trace("Map key '{}' value '{}'", fieldName, value);
      try {
        builderPutEntryMethod.invoke(builderInstance, fieldName, value);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      clearField();
    }

    @Override
    void pushString(String s) {
      pushValue(s);
    }

    @Override
    void pushBinary(byte[] bytes, int offset, int len) {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    @Override
    void pushNumber(String value) {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    @Override
    void pushNumber(long value) {
      pushValue(value);
    }

    @Override
    void pushNumber(int value) {
      pushValue(value);
    }

    @Override
    void pushNumber(float value) {
      pushValue(value);
    }

    @Override
    void pushNumber(double value) {
      pushValue(value);
    }

    @Override
    void pushNumber(BigInteger value) {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    @Override
    void pushNumber(BigDecimal value) {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    @Override
    void pushBoolean(boolean value) {
      pushValue(value);
    }

    private void clearField() {
      fieldName = null;
    }

    @Override
    void setFieldName(String fieldName) {
      LOGGER.trace("Map key '{}'", fieldName);
      this.fieldName = fieldName;
    }
  }

  final class TypedListGenContext extends GenContext {

    private final Descriptors.Descriptor type;
    private final Descriptors.FieldDescriptor field;
    private final ProtoMessageType protoMessageType;
    private final Object builderInstance;
    private final Descriptors.FieldDescriptor protoField;
    private List<?> javaList;
    private List<Object> protoList;

    TypedListGenContext(GenContext parent, Descriptors.Descriptor type) {
      super(parent);
      List<Descriptors.FieldDescriptor> fields = type.getFields();
      checkState(
          fields.size() == 1,
          "Unexpected number of fields {} in {}, expected 1",
          fields.size(),
          type.getFullName());
      Descriptors.FieldDescriptor field = fields.get(0);
      checkState(
          field.isRepeated(),
          "Expecting single field {}.{} to be 'repeated'",
          type.getFullName(),
          field.getName());
      this.type = type;
      this.field = field;

      this.protoMessageType = mapper.protoMessageType(type.getFullName());
      this.builderInstance = protoMessageType.protoBuilderSupplier().get();

      this.protoField = protoMessageType.protoFields().values().iterator().next();
      checkState(protoField != null);
      this.protoList = new ArrayList<>();
    }

    @Override
    void currentObject(Object v) {
      this.javaList = (List<?>) v;
    }

    @Override
    Object finish() {
      protoMessageType.protoBuilderSetField().setField(builderInstance, protoField, javaList);
      return protoMessageType.protoBuilderBuild().apply(builderInstance);
    }

    @Override
    void pushValue(Object value) {
      protoList.add(value);
    }

    @Override
    void pushString(String s) {
      pushValue(s);
    }

    @Override
    void pushBinary(byte[] bytes, int offset, int len) {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    @Override
    void pushNumber(String value) {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    @Override
    void pushNumber(long value) {
      pushValue(value);
    }

    @Override
    void pushNumber(int value) {
      pushValue(value);
    }

    @Override
    void pushNumber(float value) {
      pushValue(value);
    }

    @Override
    void pushNumber(double value) {
      pushValue(value);
    }

    @Override
    void pushNumber(BigInteger value) {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    @Override
    void pushNumber(BigDecimal value) {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    @Override
    void pushBoolean(boolean value) {
      pushValue(value);
    }

    @Override
    void setFieldName(String fieldName) {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }
  }

  static final class ArrayGenContext extends GenContext {
    final List<Object> list = new ArrayList<>();

    ArrayGenContext(GenContext parent) {
      super(parent);
    }

    @Override
    void currentObject(Object v) {
      value = v;
    }

    @Override
    Object finish() {
      parent.pushValue(list);
      return list;
    }

    @Override
    void pushValue(Object value) {
      list.add(value);
    }

    @Override
    void pushString(String s) {
      pushValue(s);
    }

    @Override
    void pushBinary(byte[] bytes, int offset, int len) {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    @Override
    void pushNumber(String value) {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    @Override
    void pushNumber(long value) {
      pushValue(value);
    }

    @Override
    void pushNumber(int value) {
      pushValue(value);
    }

    @Override
    void pushNumber(float value) {
      pushValue(value);
    }

    @Override
    void pushNumber(double value) {
      pushValue(value);
    }

    @Override
    void pushNumber(BigInteger value) {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    @Override
    void pushNumber(BigDecimal value) {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    @Override
    void pushBoolean(boolean value) {
      pushValue(value);
    }

    @Override
    void setFieldName(String fieldName) {
      throw new UnsupportedOperationException("IMPLEMENT ME ??");
    }
  }

  @Override
  public WritableTypeId writeTypePrefix(WritableTypeId typeIdDef) throws IOException {
    LOGGER.trace("writeTypePrefix");
    this.typeIdDef = typeIdDef;
    return super.writeTypePrefix(typeIdDef);
  }

  @Override
  public WritableTypeId writeTypeSuffix(WritableTypeId typeIdDef) throws IOException {
    LOGGER.trace("writeTypeSuffix");
    return super.writeTypeSuffix(typeIdDef);
  }

  @Override
  public void writeStartObject() {
    LOGGER.trace("writeStartObject");
    GenContext current = currentContext();
    if (current instanceof ObjectGenContext) {
      ObjectGenContext currentObject = (ObjectGenContext) current;
      if (currentObject.property.getPrimaryType().isMapLikeType()) {
        pushContext(new MapGenContext(current, currentObject.protoField.getMessageType()));
      } else {
        pushContext(new ObjectGenContext(current, typeIdDef, rootType));
      }
    } else {
      pushContext(new ObjectGenContext(current, typeIdDef, rootType));
    }
    typeIdDef = null;
  }

  @Override
  public void writeEndObject() {
    LOGGER.trace("writeEndObject");
    popContext(GenContext.class).finish();
  }

  @Override
  public void writeStartArray() {
    LOGGER.trace("writeStartArray");
    GenContext current = currentContext();
    if (current instanceof MapGenContext) {
      // For the `Map<K, List<?>>` Java type to proto `map<K, MessageType>` case: need to special
      //  case here and pull the proto/Java type in.
      MapGenContext mapGenContext = (MapGenContext) current;
      Descriptors.Descriptor type = mapGenContext.valueField.getMessageType();
      // TODO introduce a new gen-context implementation that pushes to the nested proto object type
      pushContext(new TypedListGenContext(current, type));
    } else {
      pushContext(new ArrayGenContext(currentContext()));
    }
  }

  @Override
  public void writeEndArray() {
    LOGGER.trace("writeEndArray");
    GenContext current = popContext(GenContext.class);
    currentContext().pushValue(current.finish());
  }

  @Override
  public Object getCurrentValue() {
    return super.getCurrentValue();
  }

  @Override
  public void setCurrentValue(Object v) {
    LOGGER.trace("setCurrentValue({})", v);
    GenContext current = currentContext();
    current.currentObject(v);
    super.setCurrentValue(v);
  }

  @Override
  public void writeFieldName(String fieldName) {
    currentContext().setFieldName(fieldName);
  }

  @Override
  public void writeProtoMessage(Object value) {
    LOGGER.trace("writeProtoMessage()");
    currentContext().pushValue(value);
  }

  @Override
  public void writeProtoTimestamp(Instant value) {
    LOGGER.trace("writeProtoTimestamp()");
    ProtoMessageType msgType = mapper.protoMessageType("google.protobuf.Timestamp");
    Object builder = msgType.protoBuilderSupplier().get();
    Descriptors.FieldDescriptor fieldSeconds = msgType.descriptor().findFieldByNumber(1);
    Descriptors.FieldDescriptor fieldNanos = msgType.descriptor().findFieldByNumber(2);
    msgType.protoBuilderSetField().setField(builder, fieldSeconds, value.getEpochSecond());
    msgType.protoBuilderSetField().setField(builder, fieldNanos, value.getNano());
    currentContext().pushValue(msgType.protoBuilderBuild().apply(builder));
  }

  @Override
  public void writeProtoDuration(Duration value) {
    LOGGER.trace("writeProtoDuration()");
    ProtoMessageType msgType = mapper.protoMessageType("google.protobuf.Duration");
    Object builder = msgType.protoBuilderSupplier().get();
    Descriptors.FieldDescriptor fieldSeconds = msgType.descriptor().findFieldByNumber(1);
    Descriptors.FieldDescriptor fieldNanos = msgType.descriptor().findFieldByNumber(2);
    msgType.protoBuilderSetField().setField(builder, fieldSeconds, value.getSeconds());
    msgType.protoBuilderSetField().setField(builder, fieldNanos, value.getNano());
    currentContext().pushValue(msgType.protoBuilderBuild().apply(builder));
  }

  @Override
  public void writeString(String s) {
    LOGGER.trace("writeString(String)");
    currentContext().pushString(s);
  }

  @Override
  public void writeString(char[] chars, int offset, int len) {
    LOGGER.trace("writeString(char[],int,int)");
    currentContext().pushString(new String(chars, offset, len));
  }

  @Override
  public void writeUTF8String(byte[] bytes, int offset, int len) {
    LOGGER.trace("writeUTF8String(byte[],int,int)");
    currentContext().pushString(new String(bytes, offset, len));
  }

  @Override
  public void writeRawUTF8String(byte[] bytes, int offset, int len) {
    LOGGER.trace("writeRawUTF8String(byte[],int,int)");
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeRaw(String s) {
    LOGGER.trace("writeRaw(String)");
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeRaw(String s, int offset, int len) {
    LOGGER.trace("writeRaw(String,int,int)");
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeRaw(char[] chars, int offset, int len) {
    LOGGER.trace("writeRaw(char[],int,int)");
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeRaw(char c) {
    LOGGER.trace("writeRaw(char)");
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeBinary(Base64Variant base64Variant, byte[] bytes, int offset, int len) {
    LOGGER.trace("writeBinary");
    currentContext().pushBinary(bytes, offset, len);
  }

  @Override
  public void writeNumber(int i) {
    LOGGER.trace("writeNumber(int)");
    currentContext().pushNumber(i);
  }

  @Override
  public void writeNumber(long l) {
    LOGGER.trace("writeNumber(long)");
    currentContext().pushNumber(l);
  }

  @Override
  public void writeNumber(BigInteger bigInteger) {
    LOGGER.trace("writeNumber(BigInteger)");
    currentContext().pushNumber(bigInteger);
  }

  @Override
  public void writeNumber(double v) {
    LOGGER.trace("writeNumber(double)");
    currentContext().pushNumber(v);
  }

  @Override
  public void writeNumber(float v) {
    LOGGER.trace("writeNumber(float)");
    currentContext().pushNumber(v);
  }

  @Override
  public void writeNumber(BigDecimal bigDecimal) {
    LOGGER.trace("writeNumber(BigDecimal)");
    currentContext().pushNumber(bigDecimal);
  }

  @Override
  public void writeNumber(String s) {
    LOGGER.trace("writeNumber(String)");
    currentContext().pushNumber(s);
  }

  @Override
  public void writeBoolean(boolean b) {
    LOGGER.trace("writeBoolean");
    currentContext().pushBoolean(b);
  }

  @Override
  public void writeNull() {
    LOGGER.trace("writeNull");
    currentContext().pushValue(null);
  }

  @Override
  public void flush() {
    LOGGER.trace("flush");
  }

  @Override
  protected void _releaseBuffers() {}

  @Override
  protected void _verifyValueWrite(String s) {}
}

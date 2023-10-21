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

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.base.ParserBase;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.databind.JavaType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import org.projectnessie.jackson.protobuf.api.ProtobufParser;
import org.projectnessie.nessie.relocated.protobuf.GeneratedMessageV3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ProtobufParserImpl extends ParserBase implements ProtobufParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufParserImpl.class);

  private final ProtobufMapper mapper;
  private final InputStream in;
  private final Object rootProtoObject;
  private ObjectCodec codec;
  private final Deque<ParseCtx> stack = new ArrayDeque<>();

  ProtobufParserImpl(ProtobufMapper mapper, IOContext ctxt, int parserFeatures, InputStream in) {
    super(ctxt, parserFeatures);
    this.mapper = mapper;
    this.in = in;
    this.rootProtoObject = null;
    throw new UnsupportedOperationException("IMPLEMENT THIS");
  }

  ProtobufParserImpl(
      ProtobufMapper mapper, IOContext ctxt, int parserFeatures, byte[] data, int offset, int len) {
    this(mapper, ctxt, parserFeatures, new ByteArrayInputStream(data, offset, len));
  }

  public ProtobufParserImpl(
      ProtobufMapper mapper, IOContext ctxt, int parserFeatures, Object rootProtoObject) {
    super(ctxt, parserFeatures);
    this.mapper = mapper;
    this.in = null;
    this.rootProtoObject = rootProtoObject;
  }

  @Override
  protected void _closeInput() throws IOException {}

  @Override
  public ObjectCodec getCodec() {
    return codec;
  }

  @Override
  public void setCodec(ObjectCodec objectCodec) {
    this.codec = objectCodec;
  }

  abstract class ParseCtx {
    JsonToken nextToken;

    abstract boolean canReadTypeId();

    abstract Object getTypeId();
  }

  class ObjectParseCtx extends ParseCtx {
    private final JavaType targetType;
    private final ProtoBeanType targetBeanType;
    private final ProtoMessageType targetMessageType;
    private final GeneratedMessageV3 protoObject;
    private final ProtoMessageType protoMessageType;

    ObjectParseCtx(JavaType targetType, Object protoObject) {
      this.targetType = targetType;
      this.protoObject = (GeneratedMessageV3) protoObject;
      targetBeanType = mapper.protoBeanType(targetType);
      targetMessageType = targetBeanType.messageType();
      protoMessageType = mapper.protoMessageType(this.protoObject.getDescriptorForType());
      nextToken = JsonToken.START_OBJECT;
    }

    @Override
    boolean canReadTypeId() {
      return true;
    }

    @Override
    Object getTypeId() {
      throw new UnsupportedOperationException("IMPLEMENT ME");
    }
  }

  ParseCtx ctx() {
    ParseCtx ctx = stack.peek();
    checkState(ctx != null, "Parse context stack is empty");
    return ctx;
  }

  void rootTargetType(JavaType rootTargetType) {
    LOGGER.trace("rootTargetType {}", rootTargetType);
    checkState(stack.isEmpty(), "Parse context stack not empty");
    stack.add(new ObjectParseCtx(rootTargetType, rootProtoObject));
  }

  @Override
  public Object getCurrentValue() {
    LOGGER.trace("getCurrentValue");
    return super.getCurrentValue();
  }

  @Override
  public void setCurrentValue(Object v) {
    LOGGER.trace("setCurrentValue");
    super.setCurrentValue(v);
  }

  @Override
  public JsonToken nextToken() throws IOException {
    JsonToken nextToken = ctx().nextToken;
    LOGGER.trace("nextToken returning {}", nextToken);
    return nextToken;
  }

  @Override
  public boolean canReadTypeId() {
    LOGGER.trace("canReadTypeId");
    return ctx().canReadTypeId();
  }

  @Override
  public Object getTypeId() throws IOException {
    LOGGER.trace("getTypeId");
    return ctx().getTypeId();
  }

  @Override
  public String getText() throws IOException {
    LOGGER.trace("getText");
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public Instant getProtoTimestamp() {
    LOGGER.trace("getProtoTimestamp");
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public Duration getProtoDuration() {
    LOGGER.trace("getProtoDuration");
    return null;
  }

  @Override
  public char[] getTextCharacters() throws IOException {
    LOGGER.trace("getTextCharacters");
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public int getTextLength() throws IOException {
    LOGGER.trace("getTextLength");
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }

  @Override
  public int getTextOffset() throws IOException {
    LOGGER.trace("getTextOffset");
    throw new UnsupportedOperationException("IMPLEMENT ME");
  }
}

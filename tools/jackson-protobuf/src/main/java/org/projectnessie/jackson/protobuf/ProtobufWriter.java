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

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationConfig;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import org.projectnessie.jackson.protobuf.api.ProtobufGenerator;

public final class ProtobufWriter extends ObjectWriter {
  private JavaType rootType;

  ProtobufWriter(
      ObjectMapper mapper, SerializationConfig config, JavaType rootType, PrettyPrinter pp) {
    super(checkArg(mapper), config, rootType, pp);
    this.rootType = rootType;
  }

  ProtobufWriter(ObjectMapper mapper, SerializationConfig config) {
    super(checkArg(mapper), config);
  }

  ProtobufWriter(ObjectMapper mapper, SerializationConfig config, FormatSchema s) {
    super(checkArg(mapper), config, s);
  }

  ProtobufWriter(
      ObjectWriter base,
      SerializationConfig config,
      GeneratorSettings genSettings,
      Prefetch prefetch) {
    super(checkArg(base), config, genSettings, prefetch);
  }

  ProtobufWriter(ObjectWriter base, SerializationConfig config) {
    super(checkArg(base), config);
  }

  ProtobufWriter(ObjectWriter base, JsonFactory f) {
    super(checkArg(base), f);
  }

  private static ObjectWriter checkArg(ObjectWriter base) {
    checkArgument(
        base instanceof ProtobufWriter, "base ObjectWriter must be an instance of ProtobufWriter");
    return base;
  }

  private static ObjectMapper checkArg(ObjectMapper mapper) {
    checkArgument(
        mapper instanceof ProtobufMapper,
        "referenced ObjectMapper must be an instance of ProtobufMapper");
    return mapper;
  }

  @Override
  protected ObjectWriter _new(ObjectWriter base, JsonFactory f) {
    return new ProtobufWriter(base, f);
  }

  @Override
  protected ObjectWriter _new(ObjectWriter base, SerializationConfig config) {
    if (config == _config) {
      return this;
    }
    return new ProtobufWriter(base, config);
  }

  @Override
  protected ObjectWriter _new(GeneratorSettings genSettings, Prefetch prefetch) {
    if ((_generatorSettings == genSettings) && (_prefetch == prefetch)) {
      return this;
    }
    return new ProtobufWriter(this, _config, genSettings, prefetch);
  }

  @Override
  public JsonGenerator createGenerator(OutputStream out) throws IOException {
    return configureProtoGenerator(super.createGenerator(out));
  }

  @Override
  public JsonGenerator createGenerator(OutputStream out, JsonEncoding enc) throws IOException {
    return configureProtoGenerator(super.createGenerator(out, enc));
  }

  @Override
  public JsonGenerator createGenerator(Writer w) throws IOException {
    return configureProtoGenerator(super.createGenerator(w));
  }

  @Override
  public JsonGenerator createGenerator(File outputFile, JsonEncoding enc) throws IOException {
    return configureProtoGenerator(super.createGenerator(outputFile, enc));
  }

  @Override
  public JsonGenerator createGenerator(DataOutput out) throws IOException {
    return configureProtoGenerator(super.createGenerator(out));
  }

  private JsonGenerator configureProtoGenerator(JsonGenerator generator) {
    checkArgument(
        generator instanceof ProtobufGeneratorImpl,
        "generator must be an instance of ProtobufGeneratorImpl");
    ProtobufGeneratorImpl gen = (ProtobufGeneratorImpl) generator;
    gen.setRootType(rootType);
    return generator;
  }

  public <T> T writeValueAsProtoObject(Object value, Class<T> protoType) throws IOException {
    ProtobufGenerator g = ((ProtobufFactory) _generatorFactory).createProtoObjectGenerator();
    JsonGenerator jg = (JsonGenerator) g;
    _configureGenerator(jg);
    configureProtoGenerator(jg);
    _serializerProvider
        .createInstance(_config, _serializerFactory)
        .serializeValue(jg, value, rootType);
    return g.getRootValue(protoType);
  }
}

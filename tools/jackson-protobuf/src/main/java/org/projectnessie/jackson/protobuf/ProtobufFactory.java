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

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.io.IOContext;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import org.projectnessie.jackson.protobuf.api.ProtobufGenerator;

public final class ProtobufFactory extends JsonFactory {

  @Override
  public boolean requiresPropertyOrdering() {
    return true;
  }

  @Override
  public String getFormatName() {
    return "protobuf";
  }

  @Override
  protected JsonParser _createParser(InputStream in, IOContext ctxt) throws IOException {
    ProtobufMapper mapper = (ProtobufMapper) _objectCodec;
    return new ProtobufParserImpl(mapper, ctxt, _parserFeatures, in);
  }

  @Override
  protected JsonParser _createParser(byte[] data, int offset, int len, IOContext ctxt)
      throws IOException {
    ProtobufMapper mapper = (ProtobufMapper) _objectCodec;
    return new ProtobufParserImpl(mapper, ctxt, _parserFeatures, data, offset, len);
  }

  public JsonParser _createParserForProto(Object protoObject) {
    ProtobufMapper mapper = (ProtobufMapper) _objectCodec;
    IOContext ctxt = this._createContext(this._createContentReference(protoObject), false);
    return new ProtobufParserImpl(mapper, ctxt, _parserFeatures, protoObject);
  }

  @Override
  public JsonParser createParser(Reader r) {
    throw characterSourceUnsupported();
  }

  @Override
  public JsonParser createParser(String content) {
    throw characterSourceUnsupported();
  }

  @Override
  public JsonParser createParser(char[] content) {
    throw characterSourceUnsupported();
  }

  @Override
  public JsonParser createParser(char[] content, int offset, int len) {
    throw characterSourceUnsupported();
  }

  @Override
  public JsonGenerator createGenerator(Writer w) {
    throw characterSourceUnsupported();
  }

  @Override
  protected JsonParser _createParser(Reader r, IOContext ctxt) {
    throw characterSourceUnsupported();
  }

  @Override
  protected JsonParser _createParser(
      char[] data, int offset, int len, IOContext ctxt, boolean recyclable) {
    throw characterSourceUnsupported();
  }

  @Override
  protected JsonGenerator _createGenerator(Writer out, IOContext ctxt) {
    throw characterSourceUnsupported();
  }

  @Override
  protected JsonGenerator _createUTF8Generator(OutputStream out, IOContext ctxt)
      throws IOException {
    throw characterSourceUnsupported();
  }

  @Override
  public JsonGenerator createGenerator(DataOutput out, JsonEncoding enc) throws IOException {
    return createGenerator(out);
  }

  @Override
  public JsonGenerator createGenerator(OutputStream out, JsonEncoding enc) throws IOException {
    return createGenerator(out);
  }

  @Override
  public JsonGenerator createGenerator(File f, JsonEncoding enc) throws IOException {
    return createGenerator(f);
  }

  public JsonGenerator createGenerator(File f) throws IOException {
    OutputStream out = _fileOutputStream(f);
    // true -> yes, we have to manage the stream since we created it
    IOContext ctxt = _createContext(_createContentReference(out), true);
    return _createGenerator(_decorate(out, ctxt));
  }

  @Override
  public JsonGenerator createGenerator(OutputStream out) throws IOException {
    // true -> yes, we have to manage the stream since we created it
    IOContext ctxt = _createContext(_createContentReference(out), true);
    return _createGenerator(_decorate(out, ctxt));
  }

  private JsonGenerator _createGenerator(OutputStream outputStream) {
    ProtobufMapper mapper = (ProtobufMapper) _objectCodec;
    return new ProtobufGeneratorImpl(mapper, _objectCodec, _generatorFeatures, outputStream);
  }

  public ProtobufGenerator createProtoObjectGenerator() {
    ProtobufMapper mapper = (ProtobufMapper) _objectCodec;
    return new ProtobufGeneratorImpl(mapper, _objectCodec, _generatorFeatures, null);
  }

  @Override
  protected Writer _createWriter(OutputStream out, JsonEncoding enc, IOContext ctxt) {
    throw characterSourceUnsupported();
  }

  @Override
  protected JsonParser _createParser(DataInput input, IOContext ctxt) {
    throw characterSourceUnsupported();
  }

  private UnsupportedOperationException characterSourceUnsupported() {
    return new UnsupportedOperationException(
        "Protobuf requires binary I/O. Character based sources and sinks like String, char[], Reader/Writer are not supported.");
  }
}

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

import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.Reference;
import org.projectnessie.model.proto.NessieModelTypes;
import org.projectnessie.model.proto.ReferenceMetadata;
import org.projectnessie.model.proto.StringList;
import org.projectnessie.nessie.relocated.protobuf.DescriptorProtos;
import org.projectnessie.nessie.relocated.protobuf.Descriptors;
import org.projectnessie.nessie.relocated.protobuf.GeneratedMessageV3;
import org.projectnessie.nessie.relocated.protobuf.InvalidProtocolBufferException;
import org.projectnessie.nessie.relocated.protobuf.Timestamp;

@ExtendWith(SoftAssertionsExtension.class)
public class TestProtoSerialization {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public <J, P extends GeneratedMessageV3> void typedValidations(
      Class<J> jacksonType, Class<P> protoType, J jacksonObject, P protoObject, Parser<P> parser)
      throws Exception {
    ProtobufMapper mapper =
        new ProtobufMapper()
            .registerFileDescriptor(NessieModelTypes.getDescriptor())
            .registerRelocation(
                "com.google.protobuf.", "org.projectnessie.nessie.relocated.protobuf.");

    byte[] bytes = mapper.writerFor(jacksonType).writeValueAsBytes(jacksonObject);

    P deserializedProto = parser.parse(bytes);

    P deserializedProtoObject =
        mapper.writerFor(jacksonType).writeValueAsProtoObject(jacksonObject, protoType);
    soft.assertThat(deserializedProto).isEqualTo(deserializedProtoObject).isEqualTo(protoObject);

    byte[] protoSerialized = deserializedProto.toByteArray();

    J deserializedObj1 = mapper.readValueFromProtoObject(protoObject, jacksonType);
    soft.assertThat(deserializedObj1).isEqualTo(jacksonObject);
    J deserializedObj2 = mapper.readValue(protoSerialized, jacksonType);
    soft.assertThat(deserializedObj2).isEqualTo(jacksonObject);
  }

  @FunctionalInterface
  public interface Parser<T> {
    T parse(byte[] bytes) throws InvalidProtocolBufferException;
  }

  static Stream<Arguments> typedValidations() {
    Instant authorTime = Instant.now();
    Instant commitTime = Instant.now();
    Branch branch =
        Branch.of(
            "branch",
            "1234567812345678",
            ImmutableReferenceMetadata.builder()
                .commitMetaOfHEAD(
                    CommitMeta.builder()
                        .message("hello")
                        .committer("committer")
                        .hash("1234567812345678")
                        .authorTime(authorTime)
                        .commitTime(commitTime)
                        .putProperties("prop1", "value1")
                        .putProperties("prop2", "value2")
                        .putAllProperties("prop1", Arrays.asList("value1", "value1-2"))
                        .putAllProperties("prop2", Arrays.asList("value2", "value2-2"))
                        .addAllAuthors("author")
                        .addAllAuthors("author2")
                        .addAllSignedOffBy("signedOffBy")
                        .addAllSignedOffBy("signedOffBy2")
                        .build())
                .commonAncestorHash("deadbeef")
                .numCommitsAhead(1)
                .numCommitsBehind(2)
                .numTotalCommits(3L)
                .build());
    org.projectnessie.model.proto.Branch branchProto =
        org.projectnessie.model.proto.Branch.newBuilder()
            .setName(branch.getName())
            .setHash(branch.getHash())
            .setMetadata(
                ReferenceMetadata.newBuilder()
                    .setCommitMetaOfHEAD(
                        org.projectnessie.model.proto.CommitMeta.newBuilder()
                            .setMessage("hello")
                            .setCommitter("committer")
                            .setHash("1234567812345678")
                            .setAuthorTime(
                                Timestamp.newBuilder()
                                    .setSeconds(authorTime.getEpochSecond())
                                    .setNanos(authorTime.getNano()))
                            .setCommitTime(
                                Timestamp.newBuilder()
                                    .setSeconds(commitTime.getEpochSecond())
                                    .setNanos(commitTime.getNano()))
                            .putProperties("prop1", "value1")
                            .putProperties("prop2", "value2")
                            .putAllProperties(
                                "prop1",
                                StringList.newBuilder()
                                    .addValue("value1")
                                    .addValue("value1-2")
                                    .build())
                            .putAllProperties(
                                "prop2",
                                StringList.newBuilder()
                                    .addValue("value2")
                                    .addValue("value2-2")
                                    .build())
                            .setAuthor("author")
                            .addAuthors("author")
                            .addAuthors("author2")
                            .setSignedOffBy("signedOffBy")
                            .addAllSignedOffBy("signedOffBy")
                            .addAllSignedOffBy("signedOffBy2"))
                    .setCommonAncestorHash("deadbeef")
                    .setNumCommitsAhead(1)
                    .setNumCommitsBehind(2)
                    .setNumTotalCommits(3))
            .build();

    org.projectnessie.model.proto.Reference referenceProto =
        org.projectnessie.model.proto.Reference.newBuilder().setBranch(branchProto).build();

    return Stream.of(
        // Branch model object (de)serialized as a `Reference` using `oneof` / type-IDs
        arguments(
            Reference.class,
            org.projectnessie.model.proto.Reference.class,
            branch,
            referenceProto,
            (Parser<org.projectnessie.model.proto.Reference>)
                org.projectnessie.model.proto.Reference::parseFrom),
        // Branch model object (de)serialized as a `Branch` *NOT* using `oneof` / type-IDs
        arguments(
            Branch.class,
            org.projectnessie.model.proto.Branch.class,
            branch,
            branchProto,
            (Parser<org.projectnessie.model.proto.Branch>)
                org.projectnessie.model.proto.Branch::parseFrom));
  }

  @Test
  public void foo() throws Exception {
    Descriptors.FileDescriptor fileDescriptor = NessieModelTypes.getDescriptor();
    byte[] fileDescriptorBytes = fileDescriptor.toProto().toByteArray();
    DescriptorProtos.FileDescriptorProto fileDescriptorFromBytes =
        DescriptorProtos.FileDescriptorProto.parseFrom(fileDescriptorBytes);

    soft.assertThat(fileDescriptorFromBytes).isEqualTo(fileDescriptor.toProto());
  }

  @Test
  public void bar() throws Exception {
    Collection<Descriptors.FileDescriptor> fileDescriptors =
        collectAllFileDescriptors(NessieModelTypes.getDescriptor());

    Map<String, String> relocations = new HashMap<>();
    relocations.put("com.google.protobuf.", "org.projectnessie.nessie.relocated.protobuf.");

    for (Descriptors.FileDescriptor fileDescriptor : fileDescriptors) {
      System.err.println("FileDescriptor");
      System.err.println("  full name = " + fileDescriptor.getFullName());
      System.err.println("  name = " + fileDescriptor.getName());
      System.err.println("  package = " + fileDescriptor.getPackage());
      DescriptorProtos.FileOptions options = fileDescriptor.getOptions();
      System.err.println("  java package name = " + options.getJavaPackage());
      System.err.println("  java outer class name = " + options.getJavaOuterClassname());
      System.err.println("  java multiple files = " + options.getJavaMultipleFiles());

      for (Descriptors.Descriptor messageType : fileDescriptor.getMessageTypes()) {
        System.err.println("    message type:");
        System.err.println("      name = " + messageType.getName());
        System.err.println("      full name = " + messageType.getFullName());
        System.err.println(
            "      java class = " + protoJavaClass(messageType.getName(), options, relocations));
        System.err.println("      containing type = " + messageType.getContainingType());
        System.err.println("      options = " + messageType.getOptions());
        for (Descriptors.EnumDescriptor enumType : messageType.getEnumTypes()) {
          System.err.println("      enum type:");
          System.err.println("        full name = " + enumType.getFullName());
        }
        for (Descriptors.Descriptor nestedType : messageType.getNestedTypes()) {
          System.err.println("      nested type:");
          System.err.println("        full name = " + nestedType.getFullName());
        }
        for (Descriptors.FieldDescriptor field : messageType.getFields()) {
          System.err.println("      field:");
          System.err.println("        full name = " + field.getFullName());
          System.err.println("        name = " + field.getName());
          System.err.println("        options = " + field.getOptions());
          System.err.println("        map = " + field.isMapField());
          System.err.println("        extension = " + field.isExtension());
          System.err.println("        packable = " + field.isPackable());
          System.err.println("        packed = " + field.isPacked());
          System.err.println("        required = " + field.isRequired());
          System.err.println("        optional = " + field.isOptional());
          System.err.println("        repeated = " + field.isRepeated());
          System.err.println("        json name = " + field.getJsonName());
          System.err.println(
              "        containing oneof = "
                  + (field.getContainingOneof() != null
                      ? field.getContainingOneof().getFullName()
                      : "<null>"));
          System.err.println(
              "        oneof = "
                  + (field.getRealContainingOneof() != null
                      ? field.getRealContainingOneof().getFullName()
                      : "<null>"));
          System.err.println("        java type = " + field.getJavaType());
          System.err.println("        type = " + field.getType());
          System.err.println("        lite java type = " + field.getLiteJavaType());
          System.err.println("        lite type = " + field.getLiteType());
          switch (field.getJavaType()) {
            case ENUM:
              System.err.println("        enum type = " + field.getEnumType().getFullName());
              System.err.println("        default value = " + field.getDefaultValue());
              break;
            case MESSAGE:
              System.err.println("        message type = " + field.getMessageType().getFullName());
              break;
            default:
              System.err.println("        default value = " + field.getDefaultValue());
              break;
          }
        }
        for (Descriptors.OneofDescriptor oneof : messageType.getOneofs()) {
          System.err.println("      oneof: " + oneof.getFullName());
        }
        for (Descriptors.OneofDescriptor oneof : messageType.getRealOneofs()) {
          System.err.println("      real oneof: " + oneof.getFullName());
        }
        messageType.getOptions();
        messageType.getRealOneofs();
      }
      for (Descriptors.EnumDescriptor enumType : fileDescriptor.getEnumTypes()) {
        System.err.println("    enum type:");
        System.err.println("      name = " + enumType.getName());
        System.err.println("      full name = " + enumType.getFullName());
        System.err.println(
            "      java class = " + protoJavaClass(enumType.getName(), options, relocations));
        System.err.println("      containing type = " + enumType.getContainingType());
        System.err.println("      options = " + enumType.getOptions());
        enumType.getIndex();
        enumType.getOptions();
        for (Descriptors.EnumValueDescriptor value : enumType.getValues()) {
          System.err.println("        enum value: " + value.getFullName());
          System.err.println("          name = " + value.getName());
          System.err.println("          full name = " + value.getFullName());
          System.err.println("          options = " + value.getOptions());
        }
      }
    }
  }

  private Class<?> protoJavaClass(
      String name, DescriptorProtos.FileOptions options, Map<String, String> relocations)
      throws Exception {
    String pkg = options.getJavaPackage() + '.';
    pkg = relocations.getOrDefault(pkg, pkg);

    String cls;
    if (options.getJavaMultipleFiles()) {
      cls = pkg + name;
    } else {
      cls = pkg + options.getJavaOuterClassname() + '$' + name;
    }

    return Class.forName(cls);
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

  private <T> void roundTrip(
      T obj,
      Function<T, byte[]> referenceSerialize,
      Function<byte[], T> referenceDeserialize,
      Class<T> type)
      throws Exception {
    byte[] serialized = referenceSerialize.apply(obj);
    T deserialized = referenceDeserialize.apply(serialized);
    byte[] reserialized = referenceSerialize.apply(deserialized);
    soft.assertThat(deserialized).isEqualTo(obj);
    soft.assertThat(serialized).isEqualTo(reserialized);

    ProtobufMapper protobufMapper = new ProtobufMapper();
    byte[] serialized2 = protobufMapper.writeValueAsBytes(obj);
    T deserialized2 = protobufMapper.readValue(serialized, type);
    soft.assertThat(deserialized2).isEqualTo(obj);
    T deserialized3 = protobufMapper.readValue(serialized2, type);
    soft.assertThat(deserialized3).isEqualTo(obj);
    T deserialized4 = referenceDeserialize.apply(serialized2);
    soft.assertThat(deserialized4).isEqualTo(obj);
  }

  //  @ParameterizedTest
  //  @MethodSource("references")
  //  void references(Reference reference) throws Exception {
  //    roundTrip(
  //        reference,
  //        ProtoSerialization::serializeReference,
  //        ProtoSerialization::deserializeReference,
  //        Reference.class);
  //  }
  //
  //  @ParameterizedTest
  //  @MethodSource("objs")
  //  void objs(Obj obj) throws Exception {
  //    roundTrip(
  //        obj,
  //        ProtoSerialization::serializeObj,
  //        (bytes) -> deserializeObj(obj.id(), bytes),
  //        Obj.class);
  //  }
  //
  //  @ParameterizedTest
  //  @MethodSource("previousPointers")
  //  void previousPointers(List<Reference.PreviousPointer> previousPointers) {
  //    byte[] serialized = serializePreviousPointers(previousPointers);
  //    List<Reference.PreviousPointer> deserialized = deserializePreviousPointers(serialized);
  //    soft.assertThat(deserialized).containsExactlyElementsOf(previousPointers);
  //
  //    Reference ref = reference("foo", randomObjId(), false, 42L, null, previousPointers);
  //    byte[] serializedRef = serializeReference(ref);
  //    Reference deserializedRef = deserializeReference(serializedRef);
  //
  // soft.assertThat(deserializedRef.previousPointers()).containsExactlyElementsOf(previousPointers);
  //  }
  //
  //  static Stream<List<Reference.PreviousPointer>> previousPointers() {
  //    return Stream.of(
  //        emptyList(),
  //        singletonList(previousPointer(randomObjId(), 42L)),
  //        asList(previousPointer(randomObjId(), 42L), previousPointer(randomObjId(), 101L)),
  //        asList(
  //            previousPointer(randomObjId(), 42L),
  //            previousPointer(randomObjId(), 101L),
  //            previousPointer(EMPTY_OBJ_ID, 99L)));
  //  }
  //
  //  static Stream<Reference> references() {
  //    return Stream.of(
  //        reference("a", EMPTY_OBJ_ID, false, 0L, null),
  //        reference("b", randomObjId(), false, 0L, null),
  //        reference("c", randomObjId(), true, 0L, null),
  //        reference("d", EMPTY_OBJ_ID, false, 42L, null),
  //        reference("e", randomObjId(), false, 42L, null),
  //        reference("f", randomObjId(), true, 42L, null),
  //        reference("g", EMPTY_OBJ_ID, false, 0L, randomObjId()),
  //        reference("h", randomObjId(), false, 0L, randomObjId()),
  //        reference("i", randomObjId(), true, 0L, randomObjId()),
  //        reference("j", EMPTY_OBJ_ID, false, 42L, randomObjId()),
  //        reference("k", randomObjId(), false, 42L, randomObjId()),
  //        reference("l", randomObjId(), true, 42L, randomObjId()));
  //  }
  //
  //  static Stream<Obj> objs() {
  //    return Stream.of(
  //        ref(randomObjId(), "hello", randomObjId(), 42L, randomObjId()),
  //        CommitObj.commitBuilder()
  //            .id(randomObjId())
  //            .seq(1L)
  //            .created(42L)
  //            .message("msg")
  //            .headers(EMPTY_COMMIT_HEADERS)
  //            .incrementalIndex(emptyImmutableIndex())
  //            .build(),
  //        tag(
  //            randomObjId(),
  //            "tab-msg",
  //            newCommitHeaders().add("Foo", "bar").build(),
  //            ByteString.copyFrom(new byte[1])),
  //        contentValue(randomObjId(), "cid", 0, ByteString.copyFrom(new byte[1])),
  //        stringData(
  //            randomObjId(),
  //            "foo",
  //            Compression.NONE,
  //            "foo",
  //            emptyList(),
  //            ByteString.copyFrom(new byte[1])),
  //        indexSegments(randomObjId(), emptyList()),
  //        index(randomObjId(), emptyImmutableIndex()));
  //  }
  //
  //  static ByteString emptyImmutableIndex() {
  //    ByteBuffer target = ByteBuffer.allocate(42);
  //
  //    // Serialized segment index version
  //    target.put((byte) 1);
  //
  //    target.flip();
  //    return unsafeWrap(target);
  //  }
}

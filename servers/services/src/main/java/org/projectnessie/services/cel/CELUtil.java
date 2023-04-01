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
package org.projectnessie.services.cel;

import static org.projectnessie.cel.checker.Decls.newObjectType;
import static org.projectnessie.cel.checker.Decls.newVar;

import com.google.api.expr.v1alpha1.Decl;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.tools.ScriptHost;
import org.projectnessie.cel.types.jackson.JacksonRegistry;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.RefLogResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferenceMetadata;
import org.projectnessie.versioned.KeyEntry;

/** A utility class for CEL declarations and other things. */
public final class CELUtil {

  public static final String CONTAINER = "org.projectnessie.model";
  public static final ScriptHost SCRIPT_HOST =
      ScriptHost.newBuilder().registry(JacksonRegistry.newRegistry()).build();

  public static final String VAR_REF = "ref";
  public static final String VAR_REF_TYPE = "refType";
  public static final String VAR_REF_META = "refMeta";
  public static final String VAR_COMMIT = "commit";
  public static final String VAR_ENTRY = "entry";
  public static final String VAR_PATH = "path";
  public static final String VAR_ROLE = "role";
  public static final String VAR_OP = "op";
  public static final String VAR_OPERATIONS = "operations";
  public static final String VAR_REFLOG = "reflog";

  public static final List<Decl> REFERENCES_DECLARATIONS =
      ImmutableList.of(
          newVar(VAR_COMMIT, newObjectType(CommitMeta.class.getName())),
          newVar(VAR_REF, newObjectType(Reference.class.getName())),
          newVar(VAR_REF_META, newObjectType(ReferenceMetadata.class.getName())),
          newVar(VAR_REF_TYPE, Decls.String));

  public static final List<Decl> COMMIT_LOG_DECLARATIONS =
      ImmutableList.of(
          newVar(VAR_COMMIT, newObjectType(CommitMeta.class.getName())),
          newVar(
              VAR_OPERATIONS, Decls.newListType(newObjectType(OperationForCel.class.getName()))));

  public static final List<Decl> ENTRIES_DECLARATIONS =
      ImmutableList.of(newVar(VAR_ENTRY, newObjectType(KeyEntryForCel.class.getName())));

  public static final List<Decl> AUTHORIZATION_RULE_DECLARATIONS =
      ImmutableList.of(
          newVar(VAR_REF, Decls.String),
          newVar(VAR_PATH, Decls.String),
          newVar(VAR_ROLE, Decls.String),
          newVar(VAR_OP, Decls.String));

  public static final List<Object> COMMIT_LOG_TYPES =
      ImmutableList.of(CommitMeta.class, OperationForCel.class, ContentKey.class, Namespace.class);

  @SuppressWarnings("deprecation")
  public static final List<Object> REFLOG_TYPES =
      ImmutableList.of(RefLogResponse.RefLogResponseEntry.class);

  public static final List<Object> REFERENCES_TYPES =
      ImmutableList.of(CommitMeta.class, ReferenceMetadata.class, Reference.class);

  public static final List<Object> ENTRIES_TYPES = ImmutableList.of(KeyEntryForCel.class);

  public static final CommitMeta EMPTY_COMMIT_META = CommitMeta.fromMessage("");
  public static final ReferenceMetadata EMPTY_REFERENCE_METADATA =
      ImmutableReferenceMetadata.builder().commitMetaOfHEAD(EMPTY_COMMIT_META).build();

  @SuppressWarnings("deprecation")
  public static final List<Decl> REFLOG_DECLARATIONS =
      ImmutableList.of(
          newVar(VAR_REFLOG, newObjectType(RefLogResponse.RefLogResponseEntry.class.getName())));

  private CELUtil() {}

  /**
   * Base interface for 'mirrored' wrappers exposing data to CEL expression about entities that are
   * associated with keys.
   */
  @SuppressWarnings("unused")
  public interface KeyedEntityForCel {
    List<String> getKeyElements();

    String getKey();

    String getEncodedKey();

    List<String> getNamespaceElements();

    String getName();

    String getNamespace();
  }

  /**
   * 'Mirrored' interface wrapping a {@link Operation} for CEL to have convenience fields for CEL
   * and to avoid missing fields due to {@code @JsonIgnore}.
   */
  @SuppressWarnings("unused")
  public interface OperationForCel extends KeyedEntityForCel {
    String getType();

    ContentForCel getContent();
  }

  /**
   * 'Mirrored' interface wrapping a {@link KeyEntry} for CEL to have convenience fields and
   * maintain backward compatibility to older ways of exposing this data to scripts..
   */
  @SuppressWarnings("unused")
  public interface KeyEntryForCel extends KeyedEntityForCel {
    String getContentType();
  }

  /**
   * 'Mirrored' interface wrapping a {@link Content} for CEL to have convenience fields for CEL and
   * to avoid missing fields due to {@code @JsonIgnore}.
   */
  @SuppressWarnings("unused")
  public interface ContentForCel {
    String getType();

    String getId();
  }

  /**
   * 'Mirrors' Nessie model objects for CEL.
   *
   * @param model Nessie model object
   * @return object suitable for CEL expressions
   */
  public static Object forCel(Object model) {
    if (model instanceof Content) {
      Content c = (Content) model;
      return new ContentForCel() {
        @Override
        public String getType() {
          return c.getType().name();
        }

        @Override
        public String getId() {
          return c.getId();
        }
      };
    }
    if (model instanceof Operation) {
      Operation op = (Operation) model;
      class OperationForCelImpl extends AbstractKeyedEntity implements OperationForCel {
        @Override
        protected ContentKey key() {
          return op.getKey();
        }

        @Override
        public String getType() {
          if (op instanceof Put) {
            return "PUT";
          }
          if (op instanceof Delete) {
            return "DELETE";
          }
          return "OPERATION";
        }

        @Override
        public ContentForCel getContent() {
          if (op instanceof Put) {
            return (ContentForCel) forCel(((Put) op).getContent());
          }
          return null;
        }

        @Override
        public String toString() {
          return op.toString();
        }
      }

      return new OperationForCelImpl();
    }
    if (model instanceof KeyEntry) {
      KeyEntry entry = (KeyEntry) model;
      ContentKey key = ContentKey.of(entry.getKey().getElements());
      class KeyEntryForCelImpl extends AbstractKeyedEntity implements KeyEntryForCel {
        @Override
        protected ContentKey key() {
          return key;
        }

        @Override
        public String getContentType() {
          return entry.getType().name();
        }

        @Override
        public String toString() {
          return entry.toString();
        }
      }

      return new KeyEntryForCelImpl();
    }
    return model;
  }

  private abstract static class AbstractKeyedEntity implements KeyedEntityForCel {
    protected abstract ContentKey key();

    @Override
    public List<String> getKeyElements() {
      return key().getElements();
    }

    @Override
    public String getKey() {
      return key().toString();
    }

    @Override
    public String getEncodedKey() {
      return key().toPathString();
    }

    @Override
    public String getNamespace() {
      return key().getNamespace().name();
    }

    @Override
    public List<String> getNamespaceElements() {
      return key().getNamespace().getElements();
    }

    @Override
    public String getName() {
      return key().getName();
    }
  }
}

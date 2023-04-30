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
package org.projectnessie.services.rest;

import io.smallrye.common.annotation.RunOnVirtualThread;
import java.security.Principal;
import java.util.function.Supplier;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.validation.executable.ExecutableType;
import javax.validation.executable.ValidateOnExecution;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.NamespaceApiImpl;
import org.projectnessie.versioned.VersionStore;

@RequestScoped
@jakarta.enterprise.context.RequestScoped
@ValidateOnExecution(type = ExecutableType.ALL)
@jakarta.validation.executable.ValidateOnExecution(
    type = jakarta.validation.executable.ExecutableType.ALL)
@RunOnVirtualThread
public class RestNamespaceService extends NamespaceApiImpl {
  // Mandated by CDI 2.0
  public RestNamespaceService() {
    this(null, null, null, null);
  }

  @Inject
  @jakarta.inject.Inject
  public RestNamespaceService(
      ServerConfig config,
      VersionStore store,
      Authorizer authorizer,
      Supplier<Principal> principal) {
    super(config, store, authorizer, principal);
  }
}

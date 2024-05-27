/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.api.v2;

import java.util.List;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.NessieUserInfo;
import org.projectnessie.model.RepositoryConfigResponse;
import org.projectnessie.model.UpdateRepositoryConfigRequest;
import org.projectnessie.model.UpdateRepositoryConfigResponse;

public interface ConfigApi {

  // Note: When substantial changes in Nessie API (this and related interfaces) are made
  // the API version number reported by NessieConfiguration.getMaxSupportedApiVersion()
  // should be increased as well.

  /** Get the server configuration. */
  NessieConfiguration getConfig();

  RepositoryConfigResponse getRepositoryConfig(List<String> repositoryConfigTypes);

  UpdateRepositoryConfigResponse updateRepositoryConfig(
      UpdateRepositoryConfigRequest repositoryConfigUpdate) throws NessieConflictException;

  NessieUserInfo getUserInfo();
}

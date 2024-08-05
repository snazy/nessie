/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.foundationdbtests;

import static java.lang.String.format;

import java.util.Map;
import java.util.Optional;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.foundationdb.FoundationDBBackend;
import org.projectnessie.versioned.storage.foundationdb.FoundationDBBackendFactory;
import org.projectnessie.versioned.storage.foundationdb.ImmutableFoundationDBBackendConfig;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

public class FoundationDBBackendTestFactory implements BackendTestFactory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FoundationDBBackendTestFactory.class);

  public static final String FDB_ID = "docker:docker";
  public static final int FDB_PORT = 4500;

  private GenericContainer<?> container;
  private String clusterDescription;

  @Override
  public String getName() {
    return FoundationDBBackendFactory.NAME;
  }

  @Override
  public Backend createNewBackend() throws Exception {
    return new FoundationDBBackend(
        ImmutableFoundationDBBackendConfig.builder()
            .clusterFileContents(getClusterDescription())
            .build());
  }

  @Override
  @SuppressWarnings("resource")
  public void start(Optional<String> containerNetworkId) {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    DockerImageName dockerImage =
        ContainerSpecHelper.builder()
            .name("foundationdb-local")
            .containerClass(FoundationDBBackendTestFactory.class)
            .build()
            .dockerImageName(null);

    for (int retry = 0; ; retry++) {
      GenericContainer<?> c =
          new GenericContainer<>(dockerImage)
              .withLogConsumer(new Slf4jLogConsumer(LOGGER))
              .withExposedPorts(FDB_PORT)
              .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb");
      containerNetworkId.ifPresent(c::withNetworkMode);
      try {
        c.start();
        container = c;
        break;
      } catch (ContainerLaunchException e) {
        c.close();
        if (e.getCause() != null && retry < 3) {
          LOGGER.warn("Launch of container {} failed, will retry...", c.getDockerImageName(), e);
          continue;
        }
        LOGGER.error("Launch of container {} failed", c.getDockerImageName(), e);
        throw new RuntimeException(e);
      }
    }

    Integer port = containerNetworkId.isPresent() ? FDB_PORT : container.getFirstMappedPort();
    String host =
        containerNetworkId.isPresent()
            ? container.getCurrentContainerInfo().getConfig().getHostName()
            : container.getHost();

    clusterDescription = format("%S@%s:%d", FDB_ID, host, port);
  }

  @Override
  public void start() {
    start(Optional.empty());
  }

  @Override
  public void stop() {
    try {
      if (container != null) {
        container.stop();
      }
    } finally {
      container = null;
      clusterDescription = null;
    }
  }

  public String getClusterDescription() {
    return clusterDescription;
  }

  @Override
  public Map<String, String> getQuarkusConfig() {
    return Map.of();
  }
}

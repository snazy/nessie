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
package org.projectnessie.testing.azurite;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

public class AzuriteContainer extends GenericContainer<AzuriteContainer> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AzuriteContainer.class);

  private static final int PORT = 10000;

  public static final String IMAGE_TAG;

  static {
    URL resource = AzuriteContainer.class.getResource("Dockerfile-azurite-version");
    try (InputStream in = resource.openConnection().getInputStream()) {
      String[] imageTag =
          IOUtils.readLines(in, UTF_8).stream()
              .map(String::trim)
              .filter(l -> l.startsWith("FROM "))
              .map(l -> l.substring(5).trim().split(":"))
              .findFirst()
              .orElseThrow(IllegalArgumentException::new);
      IMAGE_TAG = imageTag[0] + ':' + imageTag[1];
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract tag from " + resource, e);
    }
  }

  private static final String LOG_WAIT_REGEX = "Azurite Blob service successfully listens on .*";

  public static final String ACCOUNT = "dummyaccount";
  public static final String ACCOUNT_FQ = ACCOUNT + ".dfs.core.windows.net";
  public static final String KEY = "dummykey";
  public static final String KEY_BASE64 =
      new String(Base64.getEncoder().encode(KEY.getBytes(StandardCharsets.UTF_8)));

  public static final String STORAGE_CONTAINER = "container";

  public AzuriteContainer() {
    this(null);
  }

  public AzuriteContainer(String image) {
    super(image == null ? IMAGE_TAG : image);
    withCommand("azurite-blob", "--blobHost", "0.0.0.0");
    withLogConsumer(c -> LOGGER.info("[AZURITE] {}", c.getUtf8StringWithoutLineEnding()));
    addExposedPort(PORT);
    addEnv("AZURITE_ACCOUNTS", ACCOUNT + ":" + KEY_BASE64);
    setWaitStrategy(new LogMessageWaitStrategy().withRegEx(LOG_WAIT_REGEX));
  }

  @Override
  public void start() {
    super.start();

    LOGGER.info(
        "Azurite started with blob port {} mapped to {}, endpoint: {}",
        PORT,
        getMappedPort(PORT),
        endpoint());
  }

  public void createStorageContainer() {
    serviceClient().createFileSystem(STORAGE_CONTAINER);
  }

  public void deleteStorageContainer() {
    serviceClient().deleteFileSystem(STORAGE_CONTAINER);
  }

  public DataLakeServiceClient serviceClient() {
    return new DataLakeServiceClientBuilder()
        .endpoint(endpoint())
        .credential(credential())
        .buildClient();
  }

  public String location(String path) {
    return String.format("abfs://%s@%s/%s", STORAGE_CONTAINER, ACCOUNT_FQ, path);
  }

  public String endpoint() {
    return String.format("http://%s/%s", endpointHostPort(), ACCOUNT);
  }

  public String endpointHostPort() {
    return String.format("%s:%d", getHost(), getMappedPort(PORT));
  }

  public StorageSharedKeyCredential credential() {
    return new StorageSharedKeyCredential(ACCOUNT, KEY_BASE64);
  }

  public Map<String, String> hadoopConfig() {
    Map<String, String> r = new HashMap<>();

    r.put("fs.azure.impl", "org.apache.hadoop.fs.azure.AzureNativeFileSystemStore");
    r.put("fs.AbstractFileSystem.azure.impl", "org.apache.hadoop.fs.azurebfs.Abfs");

    r.put("fs.azure.always.use.https", "false");
    r.put("fs.azure.abfs.endpoint", endpointHostPort());

    r.put("fs.azure.account.auth.type", "SharedKey");
    r.put("fs.azure.storage.emulator.account.name", ACCOUNT_FQ);
    r.put("fs.azure.account.key." + ACCOUNT_FQ, KEY_BASE64);

    return r;
  }
}

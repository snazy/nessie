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
package org.projectnessie.client.http.impl.jdk11;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.time.Duration;
import java.util.Locale;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;

public final class JavaHttpClient implements org.projectnessie.client.http.HttpClient {
  final HttpRuntimeConfig config;
  final HttpClient client;

  public JavaHttpClient(HttpRuntimeConfig config) {
    this.config = config;

    HttpClient.Builder clientBuilder =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(config.getConnectionTimeoutMillis()));

    if (config.getSslContext() != null) {
      clientBuilder.sslContext(config.getSslContext());
    }
    if (config.getSslParameters() != null) {
      clientBuilder.sslParameters(config.getSslParameters());
    }

    if (config.getFollowRedirects() != null) {
      clientBuilder.followRedirects(
          Redirect.valueOf(config.getFollowRedirects().toUpperCase(Locale.ROOT)));
    }

    if (config.isHttp11Only()) {
      clientBuilder.version(Version.HTTP_1_1);
    }

    client = clientBuilder.build();
  }

  @Override
  public HttpRequest newRequest() {
    return new JavaRequest(this);
  }

  @Override
  public URI getBaseUri() {
    return config.getBaseUri();
  }
}

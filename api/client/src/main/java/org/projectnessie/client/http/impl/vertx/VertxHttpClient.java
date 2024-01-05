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

package org.projectnessie.client.http.impl.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.PoolOptions;
import java.net.URI;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;

public class VertxHttpClient implements org.projectnessie.client.http.HttpClient {
  final HttpRuntimeConfig config;
  private final HttpClient client;

  public VertxHttpClient(HttpRuntimeConfig config) {
    this.config = config;

    Vertx vertx = Vertx.vertx();

    HttpClientOptions clientOptions =
        new HttpClientOptions()
            .setIdleTimeoutUnit(TimeUnit.MILLISECONDS)
            .setReadIdleTimeout(config.getReadTimeoutMillis());

    if (config.getFollowRedirects() != null) {
      switch (config.getFollowRedirects().toUpperCase(Locale.ROOT)) {
        case "NORMAL":
        case "ALWAYS":
          clientOptions.setMaxRedirects(5);
          break;
        case "NEVER":
          clientOptions.setMaxRedirects(0);
          break;
      }
    }

    // TODO SSL parameters !
    // SSLContext sslContext = config.getSslContext();
    // SSLParameters sslParameters = config.getSslParameters();

    if (config.isHttp11Only()) {
      clientOptions
          .setHttp2ClearTextUpgrade(false)
          .setHttp2ClearTextUpgradeWithPreflightRequest(false)
          .setProtocolVersion(HttpVersion.HTTP_1_1);
    }

    PoolOptions poolOptions = new PoolOptions();

    this.client = vertx.createHttpClient(clientOptions, poolOptions);
  }

  @Override
  public HttpRequest newRequest() {
    return new VertxRequest(this.config, client);
  }

  @Override
  public URI getBaseUri() {
    return config.getBaseUri();
  }

  @Override
  public void close() {
    try {
      client.close().result();
    } finally {
      config.close();
    }
  }
}

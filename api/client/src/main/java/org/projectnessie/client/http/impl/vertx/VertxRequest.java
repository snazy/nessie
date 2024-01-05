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
package org.projectnessie.client.http.impl.vertx;

import static java.lang.Thread.currentThread;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;

import java.net.URL;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpTimeoutException;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;

import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpClientReadTimeoutException;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.impl.BaseHttpRequest;
import org.projectnessie.client.http.impl.HttpHeaders.HttpHeader;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;
import org.projectnessie.client.http.impl.RequestContextImpl;
import org.projectnessie.client.http.impl.jdk11.JavaResponseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implements Nessie HTTP request processing using Java's new {@link HttpClient}. */
@SuppressWarnings("Since15") // IntelliJ warns about new APIs. 15 is misleading, it means 11
final class VertxRequest extends BaseHttpRequest {

  private static final Logger LOGGER = LoggerFactory.getLogger(VertxRequest.class);

  private final HttpClient client;

  VertxRequest(HttpRuntimeConfig config, HttpClient client) {
    super(config);
    this.client = client;
  }

  @Override
  public CompletionStage<org.projectnessie.client.http.HttpResponse> executeAsync(
      Method method, Object body) throws HttpClientException {
    URI uri = uriBuilder.build();

    RequestContext context = new RequestContextImpl(headers, uri, method, body);

    HttpMethod httpMethod = HttpMethod.valueOf(method.name());
    RequestOptions requestOptions =
        new RequestOptions()
            .setMethod(httpMethod)
            .setHost(uri.getHost())
            .setPort(uri.getPort())
            .setSsl(uri.getScheme().endsWith("s"))
            .setURI(uri.getPath())
            .setConnectTimeout(config.getConnectionTimeoutMillis())
            .setTimeout(config.getReadTimeoutMillis());

    boolean doesOutput = prepareRequest(context);
    for (HttpHeader header : headers.allHeaders()) {
      for (String value : header.getValues()) {
        requestOptions.addHeader(header.getName(), value);
      }
    }

    return client
        .request(requestOptions)
        .onComplete(
            h -> {
              Future<HttpClientResponse> resp;

              if (body != null) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                writeToOutputStream(context, out);
                resp = h.send(Buffer.buffer(out.toByteArray()));
              } else {
                resp = h.send();
              }

              return resp.onComplete(
                  success -> {
                    VertxResponseContext responseContext =
                        handleResponse(method, success, context, uri);
                    success.body().map(buffer -> {

                    });
                    return config.responseFactory().make(responseContext, config.getMapper());
                  },
                  throwable -> {});
            },
            throwable -> {})
        .toCompletionStage();

    return client
        .sendAsync(request, BodyHandlers.ofInputStream())
        .handle(
            (response, e) -> {
              if (e != null) {
                if (e instanceof CompletionException) {
                  e = e.getCause();
                }
                if (e instanceof HttpConnectTimeoutException) {
                  throw new HttpClientException(
                      String.format(
                          "Timeout connecting to '%s' after %ds",
                          uri, config.getConnectionTimeoutMillis() / 1000),
                      e);
                }
                if (e instanceof HttpTimeoutException) {
                  throw new HttpClientReadTimeoutException(
                      String.format(
                          "Cannot finish %s request against '%s'. Timeout while waiting for response with a timeout of %ds",
                          method, uri, config.getReadTimeoutMillis() / 1000),
                      e);
                }
                if (e instanceof MalformedURLException) {
                  throw new HttpClientException(
                      String.format("Cannot perform %s request. Malformed Url for %s", method, uri),
                      e);
                }
                if (e instanceof IOException) {
                  throw new HttpClientException(
                      String.format("Failed to execute %s request against '%s'.", method, uri), e);
                }
                if (e instanceof RuntimeException) {
                  throw (RuntimeException) e;
                }
                throw new RuntimeException(e);
              }

              try {
                JavaResponseContext responseContext =
                    handleResponse(method, response, context, uri);

                response = null;
                return config.responseFactory().make(responseContext, config.getMapper());
              } finally {
                maybeCloseResponseBody(method, response, uri);
              }
            });
  }

  @Override
  public org.projectnessie.client.http.HttpResponse executeRequest(Method method, Object body)
      throws HttpClientException {
    URI uri = uriBuilder.build();

    RequestContext context = new RequestContextImpl(headers, uri, method, body);

    HttpRequest request = prepareRequest(method, uri, context);

    HttpResponse<InputStream> response = null;
    try {
      try {
        LOGGER.debug("Sending {} request to {} ...", method, uri);
        response = client.send(request, BodyHandlers.ofInputStream());
      } catch (HttpConnectTimeoutException e) {
        throw new HttpClientException(
            String.format(
                "Timeout connecting to '%s' after %ds",
                uri, config.getConnectionTimeoutMillis() / 1000),
            e);
      } catch (HttpTimeoutException e) {
        throw new HttpClientReadTimeoutException(
            String.format(
                "Cannot finish %s request against '%s'. Timeout while waiting for response with a timeout of %ds",
                method, uri, config.getReadTimeoutMillis() / 1000),
            e);
      } catch (MalformedURLException e) {
        throw new HttpClientException(
            String.format("Cannot perform %s request. Malformed Url for %s", method, uri), e);
      } catch (IOException e) {
        throw new HttpClientException(
            String.format("Failed to execute %s request against '%s'.", method, uri), e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      JavaResponseContext responseContext = handleResponse(method, response, context, uri);

      response = null;
      return config.responseFactory().make(responseContext, config.getMapper());
    } finally {
      maybeCloseResponseBody(method, response, uri);
    }
  }

  private static void maybeCloseResponseBody(
      Method method, HttpResponse<InputStream> response, URI uri) {
    if (response != null) {
      try {
        LOGGER.debug(
            "Closing unprocessed input stream for {} request to {} delegating to {} ...",
            method,
            uri,
            response.body());
        response.body().close();
      } catch (IOException e) {
        // ignore
      }
    }
  }

  private JavaResponseContext handleResponse(
      Method method, HttpResponse<InputStream> response, RequestContext context, URI uri) {
    JavaResponseContext responseContext = new JavaResponseContext(response);

    List<BiConsumer<ResponseContext, Exception>> callbacks = context.getResponseCallbacks();
    if (callbacks != null) {
      callbacks.forEach(callback -> callback.accept(responseContext, null));
    }

    config.getResponseFilters().forEach(responseFilter -> responseFilter.filter(responseContext));

    if (response.statusCode() >= 400) {
      // This mimics the (weird) behavior of java.net.HttpURLConnection.getResponseCode() that
      // throws an IOException for these status codes.
      throw new HttpClientException(
          String.format(
              "%s request to %s failed with HTTP/%d", method, uri, response.statusCode()));
    }
    return responseContext;
  }

  private HttpRequest prepareRequest(Method method, URI uri, RequestContext context) {
    HttpRequest.Builder request =
        HttpRequest.newBuilder().uri(uri).timeout(Duration.ofMillis(config.getReadTimeoutMillis()));

    boolean doesOutput = prepareRequest(context);

    for (HttpHeader header : headers.allHeaders()) {
      for (String value : header.getValues()) {
        request = request.header(header.getName(), value);
      }
    }

    BodyPublisher bodyPublisher = doesOutput ? bodyPublisher(context) : BodyPublishers.noBody();
    return request.method(method.name(), bodyPublisher).build();
  }

  private BodyPublisher bodyPublisher(RequestContext context) {
    ClassLoader cl = getClass().getClassLoader();
    return BodyPublishers.ofInputStream(
        () -> {
          try {
            Pipe pipe = Pipe.open();
            writerPool.execute(
                () -> {
                  ClassLoader restore = currentThread().getContextClassLoader();
                  try {
                    // Okay - this is weird - but it is necessary when running tests with Quarkus
                    // via `./gradlew :nessie-quarkus:test`.
                    currentThread().setContextClassLoader(cl);

                    writeToOutputStream(context, Channels.newOutputStream(pipe.sink()));
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  } finally {
                    currentThread().setContextClassLoader(restore);
                  }
                });
            return Channels.newInputStream(pipe.source());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Executor used to serialize the response object to JSON.
   *
   * <p>Java's new {@link HttpClient} uses the {@link Flow.Publisher}/{@link Flow.Subscriber}/{@link
   * Flow.Subscription} mechanism to write and read request and response data. We have to use that
   * protocol. Since none of the implementations must block, writes and reads run in a separate
   * pool.
   *
   * <p>Jackson has no "reactive" serialization mechanism, which means that we have to provide a
   * custom {@link OutputStream}, which delegates {@link Flow.Subscriber#onNext(Object) writes} to
   * the subscribing code.
   */
  private static final Executor writerPool =
      new ForkJoinPool(Math.max(8, ForkJoinPool.getCommonPoolParallelism()));
}

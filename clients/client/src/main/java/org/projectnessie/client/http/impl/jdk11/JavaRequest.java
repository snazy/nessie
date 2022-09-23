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

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.SubmissionPublisher;
import java.util.function.BiConsumer;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpClientReadTimeoutException;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.impl.BaseHttpRequest;
import org.projectnessie.client.http.impl.HttpHeaders.HttpHeader;
import org.projectnessie.client.http.impl.RequestContextImpl;

final class JavaRequest extends BaseHttpRequest {

  private final HttpClient client;

  JavaRequest(JavaHttpClient client) {
    super(client.config);
    this.client = client.client;
  }

  @Override
  public org.projectnessie.client.http.HttpResponse executeRequest(Method method, Object body)
      throws HttpClientException {

    URI uri = uriBuilder.build();

    HttpRequest.Builder request =
        HttpRequest.newBuilder().uri(uri).timeout(Duration.ofMillis(config.getReadTimeoutMillis()));

    RequestContext context = new RequestContextImpl(headers, uri, method, body);

    boolean doesOutput = prepareRequest(context);

    for (HttpHeader header : headers.allHeaders()) {
      for (String value : header.getValues()) {
        request = request.header(header.getName(), value);
      }
    }

    BodyPublisher bodyPublisher =
        doesOutput ? BodyPublishers.fromPublisher(publishBody(context)) : BodyPublishers.noBody();
    request = request.method(method.name(), bodyPublisher);

    HttpResponse<InputStream> response;
    try {
      response = client.send(request.build(), BodyHandlers.ofInputStream());
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

    JavaResponseContext responseContext = new JavaResponseContext(response);

    List<BiConsumer<ResponseContext, Exception>> callbacks = context.getResponseCallbacks();
    if (callbacks != null) {
      callbacks.forEach(callback -> callback.accept(responseContext, null));
    }

    config.getResponseFilters().forEach(responseFilter -> responseFilter.filter(responseContext));

    if (response.statusCode() >= 400) {
      throw new HttpClientException(
          String.format(
              "%s request to %s failed with HTTP/%d", method, uri, response.statusCode()));
    }

    return new org.projectnessie.client.http.HttpResponse(responseContext, config.getMapper());
  }

  private Flow.Publisher<ByteBuffer> publishBody(RequestContext context) {
    SubmissionPublisher<ByteBuffer> submissionPublisher =
        new SubmissionPublisher<ByteBuffer>() {
          @Override
          public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
            super.subscribe(subscriber);

            Object body = context.getBody().orElseThrow(NullPointerException::new);
            try {
              try (OutputStream out = wrapOutputStream(new SubmittingOutputStream(this))) {
                writeBody(config, out, body);
              }
              close();
            } catch (JsonGenerationException | JsonMappingException e) {
              closeExceptionally(
                  new HttpClientException(
                      String.format(
                          "Cannot serialize body of %s request against '%s'. Unable to serialize %s",
                          context.getMethod(), context.getUri(), body.getClass()),
                      e));
            } catch (Exception e) {
              closeExceptionally(e);
            }
          }
        };

    return submissionPublisher;
  }

  private static final class SubmittingOutputStream extends OutputStream {

    private final SubmissionPublisher<ByteBuffer> submissionPublisher;

    public SubmittingOutputStream(SubmissionPublisher<ByteBuffer> submissionPublisher) {
      this.submissionPublisher = submissionPublisher;
    }

    @Override
    public void write(int b) {
      // this is never called in practice, but better be on the safe side and implement it
      byte[] arr = new byte[] {(byte) b};
      write(arr, 0, 1);
    }

    @Override
    public void write(byte[] b) {
      write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      submissionPublisher.submit(ByteBuffer.wrap(Arrays.copyOfRange(b, off, off + len)));
    }
  }
}

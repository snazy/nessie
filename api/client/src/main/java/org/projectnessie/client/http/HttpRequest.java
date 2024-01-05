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
package org.projectnessie.client.http;

import java.util.concurrent.CompletionStage;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.impl.HttpHeaders;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;
import org.projectnessie.client.http.impl.UriBuilder;

/** Class to hold an ongoing HTTP request and its parameters/filters. */
public abstract class HttpRequest
    implements ExecutableHttpRequest<HttpClientException, RuntimeException> {

  protected final HttpRuntimeConfig config;
  protected final UriBuilder uriBuilder;
  protected final HttpHeaders headers = new HttpHeaders();
  protected String contentsType = "application/json; charset=utf-8";
  protected String accept = "application/json; charset=utf-8";

  protected HttpRequest(HttpRuntimeConfig config) {
    this.uriBuilder = new UriBuilder(config.getBaseUri());
    this.config = config;

    int clientSpec = config.getClientSpec();
    if (clientSpec > 0) {
      headers.put("Nessie-Client-Spec", Integer.toString(clientSpec));
    }
  }

  public HttpRequest contentsType(String contentType) {
    this.contentsType = contentType;
    return this;
  }

  public HttpRequest accept(String accept) {
    this.accept = accept;
    return this;
  }

  public HttpRequest path(String path) {
    this.uriBuilder.path(path);
    return this;
  }

  public HttpRequest queryParam(String name, String value) {
    this.uriBuilder.queryParam(name, value);
    return this;
  }

  public HttpRequest queryParam(String name, Integer value) {
    if (value != null) {
      this.uriBuilder.queryParam(name, value.toString());
    }
    return this;
  }

  public HttpRequest header(String name, String value) {
    headers.put(name, value);
    return this;
  }

  public abstract HttpResponse executeRequest(Method method, Object body)
      throws HttpClientException;

  public abstract CompletionStage<HttpResponse> executeAsync(Method method, Object body)
      throws HttpClientException;

  @Override
  public HttpResponse get() throws HttpClientException {
    return executeRequest(Method.GET, null);
  }

  @Override
  public HttpResponse delete() throws HttpClientException {
    return executeRequest(Method.DELETE, null);
  }

  @Override
  public HttpResponse post(Object obj) throws HttpClientException {
    return executeRequest(Method.POST, obj);
  }

  @Override
  public HttpResponse put(Object obj) throws HttpClientException {
    return executeRequest(Method.PUT, obj);
  }

  @Override
  public CompletionStage<HttpResponse> getAsync() throws HttpClientException {
    return executeAsync(Method.GET, null);
  }

  @Override
  public CompletionStage<HttpResponse> deleteAsync() throws HttpClientException {
    return executeAsync(Method.DELETE, null);
  }

  @Override
  public CompletionStage<HttpResponse> postAsync(Object obj) throws HttpClientException {
    return executeAsync(Method.POST, obj);
  }

  @Override
  public CompletionStage<HttpResponse> putAsync(Object obj) throws HttpClientException {
    return executeAsync(Method.PUT, obj);
  }

  /**
   * Sets the content-type to application/x-www-form-urlencoded. The provided body will be
   * automatically encoded as form data. This is a convenience method for {@code
   * contentsType("application/x-www-form-urlencoded").post(obj)}. The request should be sent with
   * {@link #post(Object)}.
   */
  public HttpResponse postForm(Object obj) {
    return contentsType("application/x-www-form-urlencoded").post(obj);
  }

  public CompletionStage<HttpResponse> postFormAsync(Object obj) {
    return contentsType("application/x-www-form-urlencoded").postAsync(obj);
  }

  public HttpRequest resolveTemplate(String name, String value) {
    uriBuilder.resolveTemplate(name, value);
    return this;
  }

  public <E extends Exception> ExecutableHttpRequest<E, RuntimeException> unwrap(Class<E> ex) {
    return new HttpRequestWrapper<>(this, ex, RuntimeException.class);
  }

  public <E1 extends Exception, E2 extends Exception> ExecutableHttpRequest<E1, E2> unwrap(
      Class<E1> ex1, Class<E2> ex2) {
    return new HttpRequestWrapper<>(this, ex1, ex2);
  }
}

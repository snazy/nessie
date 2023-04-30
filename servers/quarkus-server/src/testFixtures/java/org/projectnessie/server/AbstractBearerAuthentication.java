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
package org.projectnessie.server;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.smallrye.jwt.build.Jwt;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.auth.BearerAuthenticationProvider;
import org.projectnessie.client.rest.NessieNotAuthorizedException;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager;
import org.projectnessie.server.authn.AuthenticationEnabledProfile;

@SuppressWarnings("resource") // api() returns an AutoCloseable
public abstract class AbstractBearerAuthentication extends BaseClientAuthTest {

  protected abstract String getValidJwtToken();

  private String getInvalidJwtToken() {
    return Jwt.preferredUserName("expired")
        .issuer("https://server.example.com")
        .expiresAt(0)
        .sign();
  }

  @Test
  void testValidJwtToken() throws Exception {
    withClientCustomizer(
        b -> b.withAuthentication(BearerAuthenticationProvider.create(getValidJwtToken())));
    assertThat(api().getAllReferences().stream()).isNotEmpty();
  }

  @Test
  void testInvalidJwtToken() {
    withClientCustomizer(
        b -> b.withAuthentication(BearerAuthenticationProvider.create(getInvalidJwtToken())));
    assertThatThrownBy(() -> api().getAllReferences().stream())
        .isInstanceOfSatisfying(
            NessieNotAuthorizedException.class,
            e -> assertThat(e.getError().getStatus()).isEqualTo(401));
  }

  @Test
  void testAbsentToken() {
    assertThatThrownBy(() -> api().getAllReferences().stream())
        .isInstanceOfSatisfying(
            NessieNotAuthorizedException.class,
            e -> assertThat(e.getError().getStatus()).isEqualTo(401));
  }

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(AuthenticationEnabledProfile.AUTH_CONFIG_OVERRIDES)
          .put("quarkus.oidc.auth-server-url", "${keycloak.url}/realms/quarkus/")
          .put("quarkus.oidc.client-id", "quarkus-service-app")
          .put("quarkus.oidc.credentials", "secret")
          .put("quarkus.oidc.application-type", "service")
          .put("smallrye.jwt.sign.key.location", "privateKey.jwk") // for unit tests
          .build();
    }
  }

  public static class ProfileWithKeycloak extends Profile {
    @Override
    public List<TestResourceEntry> testResources() {
      return singletonList(new TestResourceEntry(KeycloakTestResourceLifecycleManager.class));
    }
  }
}

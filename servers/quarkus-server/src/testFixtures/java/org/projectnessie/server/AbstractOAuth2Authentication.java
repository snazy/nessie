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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.auth.NessieAuthentication;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticationProvider;
import org.projectnessie.client.auth.oauth2.OAuth2Exception;
import org.projectnessie.client.http.Status;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager;
import org.projectnessie.server.authn.AuthenticationEnabledProfile;

@SuppressWarnings("resource") // api() returns an AutoCloseable
public abstract class AbstractOAuth2Authentication extends BaseClientAuthTest {

  @Test
  void testAuthorizedClientCredentials() throws Exception {
    NessieAuthentication authentication = oauth2Authentication(clientCredentialsConfig());
    withClientCustomizer(b -> b.withAuthentication(authentication));
    assertThat(api().getAllReferences().stream()).isNotEmpty();
  }

  @Test
  void testAuthorizedPassword() throws Exception {
    NessieAuthentication authentication = oauth2Authentication(passwordConfig());
    withClientCustomizer(b -> b.withAuthentication(authentication));
    assertThat(api().getAllReferences().stream()).isNotEmpty();
  }

  /**
   * This test expects the OAuthClient to fail with a 401 UNAUTHORIZED, not Nessie. It is too
   * difficult to configure Keycloak to return a 401 UNAUTHORIZED for a token that was successfully
   * obtained with the OAuthClient.
   */
  @Test
  void testUnauthorized() {
    NessieAuthentication authentication = oauth2Authentication(wrongPasswordConfig());
    withClientCustomizer(b -> b.withAuthentication(authentication));
    assertThatThrownBy(() -> api().getAllReferences().stream())
        .isInstanceOfSatisfying(
            OAuth2Exception.class, e -> assertThat(e.getStatus()).isEqualTo(Status.UNAUTHORIZED));
  }

  protected Properties clientCredentialsConfig() {
    Properties config = new Properties();
    config.setProperty("nessie.authentication.oauth2.token-endpoint", tokenEndpoint());
    config.setProperty("nessie.authentication.oauth2.grant-type", "client_credentials");
    config.setProperty("nessie.authentication.oauth2.client-id", "quarkus-service-app");
    config.setProperty("nessie.authentication.oauth2.client-secret", "secret");
    return config;
  }

  protected Properties passwordConfig() {
    Properties config = clientCredentialsConfig();
    config.setProperty("nessie.authentication.oauth2.grant-type", "password");
    config.setProperty("nessie.authentication.oauth2.username", "alice");
    config.setProperty("nessie.authentication.oauth2.password", "alice");
    return config;
  }

  protected Properties wrongPasswordConfig() {
    Properties config = passwordConfig();
    config.setProperty("nessie.authentication.oauth2.password", "WRONG");
    return config;
  }

  protected abstract String tokenEndpoint();

  protected NessieAuthentication oauth2Authentication(Properties config) {
    return new OAuth2AuthenticationProvider().build(config::getProperty);
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

  public static class ProfileWithKeycloak extends AbstractBearerAuthentication.Profile {
    @Override
    public List<TestResourceEntry> testResources() {
      return singletonList(new TestResourceEntry(KeycloakTestResourceLifecycleManager.class));
    }
  }
}

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
package org.projectnessie.catalog.files.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class TestS3ProgrammaticOptions {

  @Test
  void normalize() {
    S3Options input =
        ImmutableS3ProgrammaticOptions.builder()
            .sessionCredentialRefreshGracePeriod(Duration.ofSeconds(1))
            .sessionCredentialCacheMaxEntries(2)
            .stsClientsCacheMaxEntries(3)
            .defaultOptions(
                ImmutableS3NamedBucketOptions.builder()
                    .endpoint(URI.create("https://host"))
                    .externalEndpoint(URI.create("https://externalHost"))
                    .pathStyleAccess(true)
                    .region("region")
                    .accessPoint("accessPoint")
                    .allowCrossRegionAccessPoint(true)
                    .serverAuthenticationMode(S3ServerAuthenticationMode.STATIC)
                    .stsEndpoint(URI.create("https://stsEndpoint"))
                    .assumeRole("assumeRole")
                    .clientAuthenticationMode(S3ClientAuthenticationMode.REQUEST_SIGNING)
                    .clientSessionDuration(Duration.ofSeconds(4))
                    .externalId("externalId")
                    .roleSessionName("roleSessionName")
                    .sessionIamPolicy("sessionIamPolicy")
                    .build())
            .putBuckets(
                "bucket1",
                ImmutableS3NamedBucketOptions.builder()
                    .endpoint(URI.create("https://host1"))
                    .externalEndpoint(URI.create("https://externalHost1"))
                    .pathStyleAccess(false)
                    .region("region1")
                    .accessPoint("accessPoint1")
                    .allowCrossRegionAccessPoint(false)
                    .serverAuthenticationMode(S3ServerAuthenticationMode.APPLICATION_GLOBAL)
                    .stsEndpoint(URI.create("https://stsEndpoint1"))
                    .assumeRole("assumeRole1")
                    .clientAuthenticationMode(S3ClientAuthenticationMode.REQUEST_SIGNING)
                    .clientSessionDuration(Duration.ofSeconds(5))
                    .externalId("externalId1")
                    .roleSessionName("roleSessionName1")
                    .sessionIamPolicy("sessionIamPolicy1")
                    .build())
            .putBuckets(
                "bucket2",
                ImmutableS3NamedBucketOptions.builder()
                    .name("my-bucket-2")
                    .endpoint(URI.create("https://host2"))
                    .externalEndpoint(URI.create("https://externalHost2"))
                    .pathStyleAccess(true)
                    .region("region2")
                    .accessPoint("accessPoint2")
                    .allowCrossRegionAccessPoint(true)
                    .serverAuthenticationMode(S3ServerAuthenticationMode.STATIC)
                    .stsEndpoint(URI.create("https://stsEndpoint2"))
                    .assumeRole("assumeRole2")
                    .clientAuthenticationMode(S3ClientAuthenticationMode.REQUEST_SIGNING)
                    .clientSessionDuration(Duration.ofSeconds(6))
                    .externalId("externalId2")
                    .roleSessionName("roleSessionName2")
                    .sessionIamPolicy("sessionIamPolicy2")
                    .build())
            .build();
    S3Options expected =
        ImmutableS3ProgrammaticOptions.builder()
            .sessionCredentialRefreshGracePeriod(Duration.ofSeconds(1))
            .sessionCredentialCacheMaxEntries(2)
            .stsClientsCacheMaxEntries(3)
            .defaultOptions(
                ImmutableS3NamedBucketOptions.builder()
                    .endpoint(URI.create("https://host"))
                    .externalEndpoint(URI.create("https://externalHost"))
                    .pathStyleAccess(true)
                    .region("region")
                    .accessPoint("accessPoint")
                    .allowCrossRegionAccessPoint(true)
                    .serverAuthenticationMode(S3ServerAuthenticationMode.STATIC)
                    .stsEndpoint(URI.create("https://stsEndpoint"))
                    .assumeRole("assumeRole")
                    .clientAuthenticationMode(S3ClientAuthenticationMode.REQUEST_SIGNING)
                    .clientSessionDuration(Duration.ofSeconds(4))
                    .externalId("externalId")
                    .roleSessionName("roleSessionName")
                    .sessionIamPolicy("sessionIamPolicy")
                    .build())
            .putBuckets(
                "bucket1",
                ImmutableS3NamedBucketOptions.builder()
                    .name("bucket1")
                    .endpoint(URI.create("https://host1"))
                    .externalEndpoint(URI.create("https://externalHost1"))
                    .pathStyleAccess(false)
                    .region("region1")
                    .accessPoint("accessPoint1")
                    .allowCrossRegionAccessPoint(false)
                    .serverAuthenticationMode(S3ServerAuthenticationMode.APPLICATION_GLOBAL)
                    .stsEndpoint(URI.create("https://stsEndpoint1"))
                    .assumeRole("assumeRole1")
                    .clientAuthenticationMode(S3ClientAuthenticationMode.REQUEST_SIGNING)
                    .clientSessionDuration(Duration.ofSeconds(5))
                    .externalId("externalId1")
                    .roleSessionName("roleSessionName1")
                    .sessionIamPolicy("sessionIamPolicy1")
                    .build())
            .putBuckets(
                "my-bucket-2",
                ImmutableS3NamedBucketOptions.builder()
                    .name("my-bucket-2")
                    .endpoint(URI.create("https://host2"))
                    .externalEndpoint(URI.create("https://externalHost2"))
                    .pathStyleAccess(true)
                    .region("region2")
                    .accessPoint("accessPoint2")
                    .allowCrossRegionAccessPoint(true)
                    .serverAuthenticationMode(S3ServerAuthenticationMode.STATIC)
                    .stsEndpoint(URI.create("https://stsEndpoint2"))
                    .assumeRole("assumeRole2")
                    .clientAuthenticationMode(S3ClientAuthenticationMode.REQUEST_SIGNING)
                    .clientSessionDuration(Duration.ofSeconds(6))
                    .externalId("externalId2")
                    .roleSessionName("roleSessionName2")
                    .sessionIamPolicy("sessionIamPolicy2")
                    .build())
            .build();
    S3Options actual = S3ProgrammaticOptions.normalize(input);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void normalizeBuckets() {
    assertThat(
            ImmutableS3ProgrammaticOptions.builder()
                .putBuckets("fs1", ImmutableS3NamedBucketOptions.builder().build())
                .putBuckets(
                    "fs2", ImmutableS3NamedBucketOptions.builder().name("my-bucket").build())
                .build()
                .buckets())
        .containsOnlyKeys("fs1", "my-bucket");
    assertThatThrownBy(
            () ->
                ImmutableS3ProgrammaticOptions.builder()
                    .putBuckets("bucket1", ImmutableS3NamedBucketOptions.builder().build())
                    .putBuckets(
                        "bucket2", ImmutableS3NamedBucketOptions.builder().name("bucket1").build())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Duplicate S3 bucket name 'bucket1', check your S3 bucket configurations");
  }
}

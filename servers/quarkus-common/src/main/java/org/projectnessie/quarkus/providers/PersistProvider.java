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
package org.projectnessie.quarkus.providers;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import io.opentelemetry.api.trace.Tracer;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.function.BiFunction;
import org.projectnessie.quarkus.config.QuarkusStoreConfig;
import org.projectnessie.quarkus.config.VersionStoreConfig;
import org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType;
import org.projectnessie.quarkus.providers.StoreType.Literal;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.storage.cache.CacheBackend;
import org.projectnessie.versioned.storage.cache.PersistCaches;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.telemetry.TelemetryPersistFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@RegisterForReflection(
    classNames = {
      "com.github.benmanes.caffeine.cache.SSSMW",
      "com.github.benmanes.caffeine.cache.PSMW"
    })
public class PersistProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(PersistProvider.class);

  private final Instance<BackendBuilder> backendBuilder;
  private final Instance<Backend> backend;
  private final VersionStoreConfig versionStoreConfig;
  private final ServerConfig serverConfig;
  private final QuarkusStoreConfig storeConfig;
  private final Instance<Tracer> opentelemetryTracer;

  @Inject
  public PersistProvider(
      @Any Instance<Tracer> opentelemetryTracer,
      @Any Instance<BackendBuilder> backendBuilder,
      @Any Instance<Backend> backend,
      VersionStoreConfig versionStoreConfig,
      QuarkusStoreConfig storeConfig,
      ServerConfig serverConfig) {
    this.backendBuilder = backendBuilder;
    this.backend = backend;
    this.versionStoreConfig = versionStoreConfig;
    this.storeConfig = storeConfig;
    this.serverConfig = serverConfig;
    this.opentelemetryTracer = opentelemetryTracer;
  }

  @Produces
  @Singleton
  @Startup
  public Backend produceBackend() {
    VersionStoreType versionStoreType = versionStoreConfig.getVersionStoreType();
    if (!versionStoreType.isNewStorage()) {
      return null;
    }

    if (backendBuilder.isUnsatisfied()) {
      throw new IllegalStateException("No Quarkus backend implementation for " + versionStoreType);
    }

    return backendBuilder.select(new Literal(versionStoreType)).get().buildBackend();
  }

  public void closeBackend(@Disposes Backend backend) throws Exception {
    if (backend != null) {
      String info = backend.configInfo();
      if (!info.isEmpty()) {
        info = " (" + info + ")";
      }
      LOGGER.info("Stopping storage for {}{}", versionStoreConfig.getVersionStoreType(), info);
      backend.close();
    }
  }

  @Produces
  @Singleton
  @Startup
  public Persist producePersist() {
    VersionStoreType versionStoreType = versionStoreConfig.getVersionStoreType();
    if (!versionStoreType.isNewStorage()) {
      return null;
    }

    if (backend.isUnsatisfied()) {
      throw new IllegalStateException("No Quarkus backend for " + versionStoreType);
    }

    Backend b = backend.get();
    b.setupSchema();

    LOGGER.info("Creating/opening version store {} ...", versionStoreType);

    BiFunction<Persist, String, Persist> wrapPersistTracing = (p, name) -> p;
    String tracingInfo = "without tracing";
    if (versionStoreConfig.isTracingEnabled()) {
      if (opentelemetryTracer.isUnsatisfied()) {
        LOGGER.warn(
            "OpenTelemetry is enabled, but not available, forgot to add quarkus-opentelemetry?");
      } else {
        Tracer t = opentelemetryTracer.get();
        TelemetryPersistFactory pf = TelemetryPersistFactory.forTracer(t);
        wrapPersistTracing = pf::wrap;
        tracingInfo = "with OpenTelemetry tracing";
      }
    }

    PersistFactory persistFactory = b.createFactory();
    Persist persist = persistFactory.newPersist(storeConfig);
    persist = wrapPersistTracing.apply(persist, persist.name());

    String info = b.configInfo();
    if (!info.isEmpty()) {
      info = " (" + info + ")";
    }

    String cacheInfo;
    int cacheCapacityMB = storeConfig.cacheCapacityMB();
    if (cacheCapacityMB > 0) {
      CacheBackend cacheBackend = PersistCaches.newBackend(1024L * 1024L * cacheCapacityMB);
      persist = cacheBackend.wrap(persist);
      persist = wrapPersistTracing.apply(persist, "Cache");
      cacheInfo = "with " + cacheCapacityMB + " MB objects cache";
    } else {
      cacheInfo = "without objects cache";
    }

    LOGGER.info("Using {} version store{}, {}, {}", versionStoreType, info, cacheInfo, tracingInfo);

    repositoryLogic(persist).initialize(serverConfig.getDefaultBranch());

    return persist;
  }
}

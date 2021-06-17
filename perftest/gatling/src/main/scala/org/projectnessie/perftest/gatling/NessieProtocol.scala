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
package org.projectnessie.perftest.gatling

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.CoreComponents
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.protocol.{Protocol, ProtocolComponents, ProtocolKey}
import io.gatling.core.scenario.Simulation
import io.gatling.core.session.Session
import io.jaegertracing.Configuration
import io.jaegertracing.internal.JaegerTracer
import io.micrometer.core.instrument.{Gauge, Tag, Timer}
import io.micrometer.prometheus.{PrometheusConfig, PrometheusMeterRegistry}
import io.opentracing.Tracer
import io.opentracing.util.GlobalTracer
import io.prometheus.client.exporter.PushGateway
import org.projectnessie.client.NessieClient

import java.time.Duration
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{
  Executors,
  ScheduledExecutorService,
  ScheduledFuture,
  TimeUnit
}
import java.util.function.Supplier
import scala.jdk.CollectionConverters._

/**  */
case class NessieProtocolBuilder(prometheusPush: Option[PrometheusPush] = None)
    extends StrictLogging {

  /** Inject the Prometheus-Push address and port, if needed. */
  def prometheusPush(
      prometheusPush: Option[PrometheusPush]
  ): NessieProtocolBuilder = NessieProtocolBuilder(prometheusPush)

  /** Inject the [[NessieClient]] for the simulation and returns an instance of [[NessieProtocol]],
    * which is to be passed to [[Simulation.SetUp.protocols()]]. */
  def client(client: NessieClient): NessieProtocol =
    NessieProtocol(client, prometheusPush)
}

case class PrometheusPush(
    jobName: String,
    pushGatewayURI: String = "127.0.0.1:9091",
    commonTags: Iterable[Tag] = Iterable.empty,
    globalTags: Iterable[Tag] = Iterable.empty
) {}

case class NessieProtocol(
    client: NessieClient,
    prometheusPush: Option[PrometheusPush]
) extends Protocol {
  type Components = NessieComponents
}

object NessieProtocol {
  val NessieProtocolKey: ProtocolKey[NessieProtocol, NessieComponents] =
    new ProtocolKey[NessieProtocol, NessieComponents] {

      def protocolClass: Class[Protocol] =
        classOf[NessieProtocol].asInstanceOf[Class[Protocol]]

      def defaultProtocolValue(
          configuration: GatlingConfiguration
      ): NessieProtocol =
        throw new IllegalStateException(
          "Can't provide a default value for NessieProtocol"
        )

      def newComponents(
          coreComponents: CoreComponents
      ): NessieProtocol => NessieComponents =
        nessieProtocol => NessieComponents(coreComponents, nessieProtocol)
    }
}

private object NessieTracer {
  val tracer: Option[Tracer] = {
    try {
      val tracer: JaegerTracer = Configuration.fromEnv().getTracer
      GlobalTracer.register(tracer)
      Some(tracer)
    } catch {
      case e: Throwable =>
        System.err.println(s"WARNING: Jaeger tracing not available: $e")
        None
    }
  }
}

/** Gatling protocol components for Nessie, nothing useful for users in here. */
case class NessieComponents(
    coreComponents: CoreComponents,
    nessieProtocol: NessieProtocol
) extends ProtocolComponents {

  val tracer: Option[Tracer] = NessieTracer.tracer

  def onStart: Session => Session = session => {
    if (nessieProtocol.prometheusPush.isDefined) {
      // Start pushing metrics via the Prometheus Push-Gateway, if the configuration is present.
      MetricsPusher.start(nessieProtocol.prometheusPush.get, session)
    } else {
      session
    }
  }

  def onExit: Session => Unit = session => {
    if (nessieProtocol.prometheusPush.isDefined) {
      // Stop pushing metrics via the Prometheus Push-Gateway
      MetricsPusher.stop(session)
    }
  }
}

/** Helper class for Metrics that are pushed to Prometheus using a Push-Gateway, provides a
  * shutdown-hook to push the "last" metrics.
  */
private case class MetricsInfo(
    push: PrometheusPush,
    totalSample: Timer.Sample,
    pushGateway: PushGateway,
    executor: ScheduledExecutorService,
    pusher: ScheduledFuture[_],
    registry: PrometheusMeterRegistry,
    currentUsers: AtomicInteger = new AtomicInteger()
) {
  def shutdown(session: Session): Unit = this.synchronized {
    if (!executor.isShutdown) {
      totalSample.stop(
        Timer
          .builder("nessie.benchmark.total-duration")
          .tags(push.commonTags.asJava)
          .tag("scenario", session.scenario)
          .tag("status", session.status.name)
          .distributionStatisticBufferLength(3)
          .distributionStatisticExpiry(Duration.ofMinutes(1))
          .register(registry)
      )

      pusher.cancel(true)
      executor.shutdown()

      pushGateway.pushAdd(registry.getPrometheusRegistry, push.jobName)
    }
  }
}

/** Internal code, pushes metrics to Prometheus via [[PushGateway]]. */
private object MetricsPusher {

  private val metricsInfo = new AtomicReference[MetricsInfo]()

  def start(push: PrometheusPush, session: Session): Session =
    this.synchronized {
      if (metricsInfo.get() == null) {
        val executor = Executors.newScheduledThreadPool(1)
        val registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val totalSample = Timer.start()
        val pushGateway = new PushGateway(push.pushGatewayURI)

        val pusher = executor.scheduleAtFixedRate(
          new Runnable {
            override def run(): Unit = {
              try {
                pushGateway
                  .pushAdd(registry.getPrometheusRegistry, push.jobName)
              } catch {
                case e: Exception =>
                  e.printStackTrace()
              }
            }
          },
          0,
          3,
          TimeUnit.SECONDS
        )

        val m = MetricsInfo(
          push,
          totalSample,
          pushGateway,
          executor,
          pusher,
          registry
        )

        Gauge
          .builder("nessie.benchmark.active-users", new Supplier[Number] {
            override def get(): Number = m.currentUsers.get
          })
          .tags(push.commonTags.asJava)
          .tags(push.globalTags.asJava)
          .tag("scenario", session.scenario)
          .register(registry)

        Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
          override def run(): Unit = {
            m.currentUsers.set(0)
            m.shutdown(session)
          }
        }))

        metricsInfo.set(m)
      }

      val m = metricsInfo.get()
      m.currentUsers.incrementAndGet()
      session.set("prometheus.registry", m.registry)
    }

  def stop(session: Session): Session = this.synchronized {
    val m = metricsInfo.getAndSet(null)
    if (m != null && m.currentUsers.decrementAndGet() == 0) {
      m.shutdown(session)
    }

    if (session.contains("prometheus.registry")) {
      session.remove("prometheus.registry")
    } else {
      session
    }
  }
}

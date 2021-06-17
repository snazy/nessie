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

import io.micrometer.core.instrument.Tag

/** Common parameters for Nessie Gatling simulations. */
trait BaseParams {

  /** The number of users to simulate. System property: `sim.users`, defaults to `1`. */
  def numUsers: Int

  /** The operation-rate in ops-per-second, `0` means "as fast as possible".
    * Implemented with Gatling's [[io.gatling.core.structure.Pauses#pace(Duration)]]
    * function, which acts like a "sleep()" after each commit.
    * System property: `sim.rate`, defaults to `0`.
    */
  def opRate: Double

  /** The IP and port of the Prometheus Push-Gateway.
    * A "benchmark_description" tag is added to all metrics, see the note-property.
    * System property: `sim.prometheus`, defaults to `null`, which means to not push to Prometheus.
    */
  def prometheusPushURL: Option[String]

  /** Arbitrary note added as a Prometheus tag, used to distinguish metrics from different runs in e.g. Grafana. System property: `sim.note`, defaults to `""`. */
  def note: String

  def asPrintableString(): String = {
    s"""
    |Simulation parameters:
    |   note:           $note
    |   num-users:      $numUsers
    |   op-rate:        $opRate
    |""".stripMargin
  }

  /**
    * If the [[prometheusPushURL]] is configured, return the [[PrometheusPush]] configured with
    * the Prometheus tags for this simulation.
    */
  def setupPrometheusPush: Option[PrometheusPush] = {
    prometheusPushURL.map(
      url =>
        PrometheusPush(
          nameForPrometheus,
          url,
          commonPrometheusTags,
          globalPrometheusTags
        )
    )
  }

  /** Should be overridden. */
  def nameForPrometheus: String = "nessie_simulation"

  def globalPrometheusTags: Seq[Tag] = {
    Seq()
  }

  def commonPrometheusTags: Seq[Tag] = {
    Seq(Tag.of("application", "Nessie-Benchmark"))
  }
}

case class BaseParamsImpl(
    override val numUsers: Int,
    override val opRate: Double,
    override val prometheusPushURL: Option[String],
    override val note: String
) extends BaseParams

object BaseParams {
  def fromSystemProperties(): BaseParams = {
    val numUsers: Int = Integer.getInteger("sim.users", 1).toInt
    val opRate: Double = System.getProperty("sim.rate", "0").toDouble
    val prometheusPushURL: Option[String] = Option(
      System.getProperty("sim.prometheus")
    )
    val note: String = System.getProperty("sim.note", "")

    BaseParamsImpl(
      numUsers,
      opRate,
      prometheusPushURL,
      note
    )
  }
}

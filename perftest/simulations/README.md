# Gatling Simulations for Nessie

Each simulation shall consist of a Scala case-class holding the simulation parameters and
the actual simulation class.

Simulation classes must extend `io.gatling.core.scenario.Simulation.Simulation`,
parameter case-classes should leverage `org.projectnessie.perftest.gatling.BaseParams`.

The Gatling Simulations for Nessie use the Nessie DSL/protocol implemented in the 
`nessie-perftest-gatling` module.

## Nessie-Gatling simulation tutorial

The main thing a Nessie-Gatling simulation needs is the setup code.

```scala
class SimpleSimulation extends Simulation {

  // Construct an instance of the SimulationParams, taking the parameters from
  // the system properties is the easiest way.
  val params: SimulationParams = SimulationParams.fromSystemProperties()

  // Construct the `NessieProtocol`, which will provide the given `NessieClient`
  // to the actions/executions.
  val nessieProtocol: NessieProtocol = nessie()
    .prometheusPush(params.setupPrometheusPush)
    .client(
      NessieClient
        .builder()
        .withUri("http://127.0.0.1:19120/api/v1")
        .fromSystemProperties()
        .withTracing(params.prometheusPushURL.isDefined)
        .build()
    )

  // Simple scenario to get the default branch N times.
  val scenario = scenario("Get-Default-Branch-100-Times")
    .repeat(params.gets, exec(
      nessie(s"Get main branch")
        .execute { (client, session) =>
          val defaultBranch = client.getTreeApi.getDefaultBranch
          session.set("defaultBranch", defaultBranch)
        }))

  // Let the simulation run the scenario above for the configured number of users.
  val s: SetUp = setUp(scenario.inject(atOnceUsers(params.numUsers)))

  // Returns the "finished" `SetUp` with the `nessieProtocol` back to Gatling.
  // (Note: yes, this is is a "return statement".) 
  s.protocols(nessieProtocol)
}

// Case class with all the options/parameters your simulation needs.
case class SimulationParams(
                             gets: Int,
                             override val numUsers: Int,
                             override val opRate: Double,
                             override val prometheusPushURL: Option[String],
                             override val note: String
                           ) extends BaseParams

// Object for the case class - to provide the `fromSystemProperties()` function.
object SimulationParams {
  def fromSystemProperties(): SimulationParams = {
    val base = BaseParams.fromSystemProperties()
    val gets: Int = Integer.getInteger("sim.gets", 100).toInt
    SimulationParams(
      gets,
      base.numUsers,
      base.opRate,
      base.prometheusPushURL,
      base.note
    )
  }
}
```

## Gatling

Gatling simulations define scenarios. A scenario is a sequence of potentially nested and conditional
executions. You can think of a scenario ~= a use-case, something like "a user logs into a shopping
website, looks through the avaialable products, adds some items to the basket, checks out & pays".

Such scenarios often don't run "as fast as possible", but have some pauses and/or conditions
and/or repetitions.

Simulations can then use those scenarios and simulate many users running those scenarios, with
various options to ramp-up the number of users, ramp-down, etc.

Important note: The Gatling `Session` object is _immutable_! This means, when you add some attribute
to the `Session`, you receive a cloned object, which needs to be passed downstream in your code or,
as in most cases, returned form the actions' code.

## Gatling links

* [Intro to Gatling](https://www.baeldung.com/introduction-to-gatling)
* [Quickstart](https://gatling.io/docs/gatling/tutorials/quickstart/)
* [Advanced Tutorial](https://gatling.io/docs/gatling/tutorials/advanced/)







# Performance testing Nessie with Gatling

## Setup

1. Follow the instructions in `perftest/measurement-pack/README.md` to start the "measurement-pack".
   "Measurement-Pack" is a Docker Compose setup that has all metrics+traces collection services
   as pre-configured Docker containers, Nessie server + Gatling tests run on the host to allow
   shorter turn-around times during development and debugging of both Nessie and the Gatling tests.
1. Build Nessie: go to the Nessie root directory and execute `./mvnw clean install`, activate the
   `-Pnative` profile, when testing a Graal native image.
1. Start the Nessie Server like you need it. In all cases you have to set these 
   environment variables:
    - `QUARKUS_JAEGER_ENDPOINT=http://127.0.0.1:14268/api/traces`
    - Without tracing: `QUARKUS_JAEGER_SAMPLER_TYPE=const` + `QUARKUS_JAEGER_SAMPLER_PARAM=0`
    - Everything traced: `QUARKUS_JAEGER_SAMPLER_TYPE=const` + `QUARKUS_JAEGER_SAMPLER_PARAM=1`
    - Probabilistic tracing: `QUARKUS_JAEGER_SAMPLER_TYPE=probabilistic` +
      `QUARKUS_JAEGER_SAMPLER_PARAM=0.01` (or any other value)
   Then run the Nessie server:
    - In your IDE in a Debugger...
    - From the command line as a Quarkus image (e.g. `java -jar servers/quarkus-server/target/quarkus-app/quarkus-run.jar`)
1. Run the Gatling test you'd like to run using the `gatling:test` in Maven for the
   `:nessie-perftest-gatling` project. See the "Full example" below for a list of parameters passed
   as system properties.

## `CommitToBranchSimulation`

| Parameter | Default value | Meaning
| --- | --- | ---
| sim.users | 1 |  number of users to simulate
| sim.commits | 100 | number of commits each simulated users adds
| sim.mode | BRANCH_PER_USER_SINGLE_TABLE | BRANCH_PER_USER_SINGLE_TABLE (default), BRANCH_PER_USER_RANDOM_TABLE, SINGLE_BRANCH_RANDOM_TABLE, SINGLE_BRANCH_TABLE_PER_USER, SINGLE_BRANCH_SINGLE_TABLE
| sim.branch | n/a | name of the branch to commit to (defaults to a name containing System.currentTimeMillis())
| sim.tablePrefix | n/a | prefix of the table name used in the commits (defaults to a name prefix containing System.currentTimeMillis())
| sim.prometheus | n/a | hostname/IP + port of the Prometheus push gateway server, `127.0.0.1:9091` is probably the local one
| sim.duration.seconds | 0 | maximum duration of the test in seconds (0 = not applied)
| sim.rate | 0 | rate of operations (0 = not applied / as fast as possible)
| sim.note | | arbitrary string that will be included in the prometheus metrics information
| http.maxConnections | 5 | set at least to the numebr of sim.users, otherwise HTTP requests will contend on the client side

## Full example

To use the local DynamoDB endpoint, check the environment variables defined in the
[`.env` file in measurement-pack](../measurement-pack/.env) before running the Nessie Server.
Adjust the environment variables via your shell, if necessary.

### Start Nessie

There are three alternatives:

#### Alternative A: Run Nessie via the "measurement-pack":

Nessie Server, latest released Docker image, is started via the default 
[`docker-compose.yml`](../measurement-pack/docker-compose.yml)
from the "measurement pack". See the `README.md` [there](../measurement-pack/README.md)

#### Alternative B: Run Nessie as a native image:

Use the alternative Docker Compose configuration
[`docker-compose-local-nessie.yml`](../measurement-pack/docker-compose-local-nessie.yml)

You should set the environment variables defined in the
[`.env` file in measurement-pack](../measurement-pack/.env).

```shell
# Set the neccessary environment variables, see ../measurement-pack/.env
servers/quarkus-server/target/nessie-quarkus-*-SNAPSHOT-runner
```

#### Alternative C: Run Nessie in a JVM:

Use the alternative Docker Compose configuration
[`docker-compose-local-nessie.yml`](../measurement-pack/docker-compose-local-nessie.yml)

You should set the environment variables defined in the
[`.env` file in measurement-pack](../measurement-pack/.env).

```shell
# Set the neccessary environment variables, see ../measurement-pack/.env
java -Xms6g -Xmx6g -XX:+AlwaysPreTouch -jar servers/quarkus-server/target/quarkus-app/quarkus-run.jar
```

### To start the Gatling simulation w/ tracing:
```shell
export JAEGER_ENDPOINT=http://127.0.0.1:14268/api/traces
export JAEGER_SAMPLER_TYPE=probabilistic
export JAEGER_SAMPLER_PARAM=0.01
export JAEGER_SERVICE_NAME=nessie
./mvnw install gatling:test -Dgatling.simulationClass=org.projectnessie.perftest.gatling.CommitToBranchSimulation -pl :nessie-perftest-gatling
```

## Notes

Please note, that `Engine`, `IDEPathHelper` and `Recorder`in `src/test/scala` and
`src/test/resources/gatling.conf` are generated by the Maven archetype and technically not required
to run a simulation, but are required to actually debug a simulation, in which case you have to run
the `Engine` class.

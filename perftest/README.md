# Running Benchmarks against Nessie

Currently, Nessie performance testing consists of two main components:
* The "measurement pack", which is a collection of Docker images using Docker Compose that contains
  a local DynamoDB mock, Prometheus, Grafana and some more.
* The Gatling scenario and simulation to simulate commits against Nessie.

Nessie currently itself runs as a separate instance, so you have to explicitly start it in the
configuration you want run. This is convenient to try and test different configuration and code
changes locally.

For more information, look at the `README.md` files in the sub-modules `measurement-pack` and `gatling`.

## Example against local DynamoDB

```shell
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx
export QUARKUS_DYNAMODB_AWS_REGION=us-west-2
export QUARKUS_DYNAMODB_ENDPOINT_OVERRIDE=http://127.0.0.1:8000
export NESSIE_SERVER_SEND_STACKTRACE_TO_CLIENT=FALSE
export NESSIE_VERSION_STORE_TYPE=DYNAMO
export NESSIE_VERSION_STORE_DYNAMO_INITIALIZE=true
export NESSIE_VERSION_STORE_DYNAMO_CACHE_ENABLED=false
export NESSIE_VERSION_STORE_DYNAMO_COMMIT_BACKOFF_RETRY_SLEEP=PT0S
export NESSIE_VERSION_STORE_DYNAMO_P2_COMMIT_BACKOFF_RETRY_SLEEP=PT0S
export QUARKUS_JAEGER_ENDPOINT=http://127.0.0.1:14268/api/traces 
export QUARKUS_JAEGER_SAMPLER_TYPE=const
export QUARKUS_JAEGER_SAMPLER_PARAM=0
# Don't emit the HTTP access log
export HTTP_ACCESS_LOG_LEVEL=WARN

./mvnw clean install -DskipTests

java -Xms6g -Xmx6g -XX:+AlwaysPreTouch -jar servers/quarkus-server/target/quarkus-app/quarkus-run.jar
```

## Example against AWS / EC2 / DynamoDB

Contents of `~/.aws/config`:
```
[default]
region=us-west-2
output=json
```

```shell
# Assumes AWS CLI credentials are in ~/.aws/credentials or the necessary env vars are configured.

#       m5d.8xlarge   32 vCPU,    128 G RAM,    2x 600 G NVMe SSD
#       m5d.4xlarge   16          64            2x 300
# -->   m5d.12xlarge  48          192           2x 900
#       m5d.16xlarge  64          256           4x 600
#       m5d.24xlarge  96          384           4x 900
#       m5d.metal     96          384           4x 900
#       m5dn...
```

## Provision the load-driver EC2 instance

`ssh` into the provisioned EC2 instance (the following assumes Ubuntu 20.04 Server).

### Initial provisioning scripts

#### Initial provisioning script when building Nessie on the EC2 instance
```shell

# Mandatory stuff (Docker, compiler, etc)
sudo apt-get update
sudo apt-get install -y wget git gcc g++ zlib1g-dev docker.io docker-compose
sudo usermod -a -G docker ubuntu

# Provision the NVMe SSD, mount in /home/ubuntu/nvm
mkdir nvm
sudo mkfs.ext4 -E nodiscard /dev/nvme1n1
sudo mount /dev/nvme1n1 /home/ubuntu/nvm
sudo chown ubuntu: nvm

cd nvm

# Put the Docker work directory on the NVMe SSD
sudo mkdir -p var-lib-docker
sudo ln -s $(pwd)/var-lib-docker /var/lib/docker

# Download and install GrallVM CE
wget https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-21.0.0.2/graalvm-ce-java11-linux-amd64-21.0.0.2.tar.gz
tar xfz graalvm-ce-java11-linux-*.tar.gz
ln -s $(find . -name "graalvm-ce-java11*" -type d -maxdepth 1) jdk

# Update environment
export JAVA_HOME=$(pwd)/jdk
export PATH=$JAVA_HOME/bin:$PATH

cat >> ~/.bashrc <<!

export JAVA_HOME=$JAVA_HOME
export PATH=\$JAVA_HOME/bin:\$PATH
!

# Install the GraalVM 'native-image' extension
gu install native-image

# Clone Nessie (this is from my personal branch!)
git clone -o snazy -b trace-commit https://github.com/snazy/nessie.git nessie
cd nessie

# Prepare the directory receiving the Prometheus data (so we can download it)
sudo mkdir perftest/measurement-pack/prometheus-data
sudo chmod o+w perftest/measurement-pack/prometheus-data

cat >> ~/.bashrc <<!

# Environment for the nessie server
export NESSIE_VERSION_STORE_TYPE=DYNAMO
export NESSIE_VERSION_STORE_DYNAMO_INITIALIZE=false
export NESSIE_VERSION_STORE_DYNAMO_CACHE_ENABLED=true
export NESSIE_VERSION_STORE_DYNAMO_COMMIT_BACKOFF_RETRY_SLEEP=PT0S
export NESSIE_VERSION_STORE_DYNAMO_P2_COMMIT_BACKOFF_RETRY_SLEEP=PT0S
export QUARKUS_DYNAMODB_AWS_REGION=us-west-2
export QUARKUS_DYNAMODB_ENDPOINT_OVERRIDE=http://dynamodb.${QUARKUS_DYNAMODB_AWS_REGION}.amazonaws.com
export QUARKUS_LOG_FILE_ENABLE=true
export QUARKUS_JAEGER_ENDPOINT=http://127.0.0.1:14268/api/traces 
export QUARKUS_JAEGER_SAMPLER_TYPE=const
export QUARKUS_JAEGER_SAMPLER_PARAM=0

# Environment for Gatling tests
export JAEGER_ENDPOINT=http://127.0.0.1:14268/api/traces
export JAEGER_SAMPLER_TYPE=const
export JAEGER_SAMPLER_PARAM=0
export JAEGER_SERVICE_NAME=nessie 
!

# Build Nessie
./mvnw clean install -DskipTests -Pnative

echo "GROUP docker ADDED TO USER ubuntu. LOGOUT AND LOGIN AGAIN !"
```

#### Initial provisioning script (not building Nessie on the EC2 instance)
```shell

# Mandatory stuff (Docker, compiler, etc)
suto apt-get update
sudo apt-get install -y git docker.io docker-compose
sudo usermod -a -G docker ubuntu

# Provision the NVMe SSD, mount in /home/ubuntu/nvm
sudo mkfs.ext4 -E nodiscard /dev/nvme1n1
sudo mount /dev/nvme1n1 /home/ubuntu/nvm
sudo chown ubuntu: nvm

cd nvm

# Put the Docker work directory on the NVMe SSD
sudo mkdir -p var-lib-docker
sudo ln -s $(pwd)/var-lib-docker /var/lib/docker

# Download and install GrallVM CE (or any other Java 11 compatible JVM) 
wget https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-21.0.0.2/graalvm-ce-java11-linux-amd64-21.0.0.2.tar.gz
tar xfz graalvm-ce-java11-linux-*.tar.gz
ln -s $(find . -name "graalvm-ce-java11*" -type d -maxdepth 1) jdk

# Update environment
export JAVA_HOME=$(pwd)/jdk
export PATH=$JAVA_HOME/bin:$PATH

cat >> ~/.bashrc <<!

export JAVA_HOME=$JAVA_HOME
export PATH=\$JAVA_HOME/bin:\$PATH
!

# Clone Nessie (this is from my personal branch!)
git clone -o snazy -b trace-commit https://github.com/snazy/nessie.git nessie
cd nessie

# Prepare the directory receiving the Prometheus data (so we can download it)
sudo mkdir perftest/measurement-pack/prometheus-data
sudo chmod o+w perftest/measurement-pack/prometheus-data

cat >> ~/.bashrc <<!

# Environment for the nessie server
export NESSIE_VERSION_STORE_TYPE=DYNAMO
export NESSIE_VERSION_STORE_DYNAMO_INITIALIZE=false
export NESSIE_VERSION_STORE_DYNAMO_CACHE_ENABLED=true
export NESSIE_VERSION_STORE_DYNAMO_COMMIT_BACKOFF_RETRY_SLEEP=PT0S
export NESSIE_VERSION_STORE_DYNAMO_P2_COMMIT_BACKOFF_RETRY_SLEEP=PT0S
export QUARKUS_DYNAMODB_AWS_REGION=us-west-2
export QUARKUS_DYNAMODB_ENDPOINT_OVERRIDE=http://dynamodb.${QUARKUS_DYNAMODB_AWS_REGION}.amazonaws.com
export QUARKUS_LOG_FILE_ENABLE=false
export QUARKUS_JAEGER_ENDPOINT=http://127.0.0.1:14268/api/traces
# Disable Jaeger tracing for our benchmark 
export QUARKUS_JAEGER_SAMPLER_TYPE=const
export QUARKUS_JAEGER_SAMPLER_PARAM=0

# Environment for Gatling tests
export JAEGER_ENDPOINT=http://127.0.0.1:14268/api/traces
export JAEGER_SERVICE_NAME=nessie 
# Disable Jaeger tracing for our benchmark 
export JAEGER_SAMPLER_TYPE=const
export JAEGER_SAMPLER_PARAM=0
!

echo "GROUP docker ADDED TO USER ubuntu. LOGOUT AND LOGIN AGAIN !"
```

### Start the "measurement pack"
```shell
# On the EC2 instance
cd nvm/nessie

cd perftest/measurement-pack

mkdir -p prometheus-data/data
chmod -R o+w prometheus-data

docker-compose up
```

### Start Nessie
```shell
# On the EC2 instance
cd nvm/nessie

# TODO replace with your credentials! See notes below!
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx

# NOTE: Path is valid when building the native image locally.
servers/quarkus-server/target/nessie-quarkus-*-SNAPSHOT-runner
```

### Start the benchmark
```shell
# On the EC2 instance
cd nvm/nessie

# See perftest/gatling/README.md for the perftest parameters.

##############################
## Tests follow 
##############################

./mvnw install gatling:test -Dgatling.simulationClass=org.projectnessie.perftest.gatling.CommitToBranchSimulation \
  -Dsim.users=5 \
  -Dsim.commits=0 \
  -Dsim.branchMode=BRANCH_PER_USER \
  -Dsim.duration.seconds=600 \
  -Dsim.rate=10 \
  -Dhttp.maxConnections=100 \
  -pl :nessie-perftest-gatling ; sleep 60 ; \
\
./mvnw install gatling:test -Dgatling.simulationClass=org.projectnessie.perftest.gatling.CommitToBranchSimulation \
  -Dsim.users=20 \
  -Dsim.commits=0 \
  -Dsim.branchMode=BRANCH_PER_USER \
  -Dsim.duration.seconds=600 \
  -Dsim.rate=10 \
  -Dhttp.maxConnections=100 \
  -pl :nessie-perftest-gatling ; sleep 60 ; \
\
./mvnw install gatling:test -Dgatling.simulationClass=org.projectnessie.perftest.gatling.CommitToBranchSimulation \
  -Dsim.users=5 \
  -Dsim.commits=0 \
  -Dsim.branchMode=SINGLE_BRANCH_TABLE_PER_USER \
  -Dsim.duration.seconds=600 \
  -Dsim.rate=10 \
  -Dhttp.maxConnections=100 \
  -pl :nessie-perftest-gatling
```

### Stop

After running the benchmarks, stop the Docker(-compose) containers

### Pull Prometheus data onto your local machine
```shell
# GO TO YOUR LOCAL NESSIE CLONE
cd perftest/measurement-pack
rm -rf prometheus-data
scp -r -i ~/.ssh/${YOUR_PRIVATE_KEY_FILE} ubuntu@${INSTANCE_IP}:nvm/nessie/perftest/measurement-pack/prometheus-data .
chmod -R o+w prometheus-data
docker-compose up
xdg-open http://127.0.0.1:3000/
```

# Notes

## Creating an AWS/IAM access-key

```shell
aws iam create-user --user-name nessie-perftest
# requires a pre-existing group dynamodb-full
aws iam add-user-to-group --group-name dynamodb-full --user-name nessie-perftest
aws iam create-access-key --user nessie-perftest

# Setup the credentials for the Nessie instance being run using the values returned by "create-access-key"
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx

# Cleanup
aws iam delete-access-key --access-key-id ${AWS_ACCESS_KEY} --user-name nessie-perftest
```

## ssh into the EC2 instance

```
ssh -L 9090:127.0.0.1:9090 -L 3000:127.0.0.1:3000 -L 19120:127.0.0.1:19120 -i ~/.ssh/<YOUR-SSH-PRIVATE-KEY> ubuntu@<INSTANCE-IP>

ssh -i ~/.ssh/<YOUR-SSH-PRIVATE-KEY> ubuntu@<INSTANCE-IP>
```

## DynamoDB capacities

* all tables: reads+writes 500
* l1, l2, refs: reads+writes 2000

Whether the commit-rate flattens up to the reserved read/write capacity or whether it will
error out a lot depends on the reserved capacities. If it can read fast enough, it will hit
write capacity limits and then error out. It also depends on the "external pressure" (the number
of concurrent commits/writes and request rate).

Feels that limit the l1, l2 + refs tables need approx 4x read+write capacity.

# Observations

## `-Dsim.users=20   -Dsim.commits=10000   -Dsim.branch=u20-c10000-uncontended-5   -Dsim.multipleBranches=true  -Dsim.prometheus=127.0.0.1:9091   -Dsim.commitLog=false   -Dhttp.maxConnections=100`

* Okay with r+w capacity=500 + l1,l2,refs=2000/2000
* DynamoDB conditional-update failures
* Throttling on all relevant tables

## `-Dsim.users=20   -Dsim.commits=10000   -Dsim.branch=u20-c10000-contended-6   -Dsim.multipleBranches=false  -Dsim.prometheus=127.0.0.1:9091   -Dsim.commitLog=false   -Dhttp.maxConnections=100`

* r+w capacity=500 + l1,l2,refs=2000/2000
* `org.projectnessie.error.NessieNotFoundException: Failed to commit data. Unable to find requested ref for commit.` (many)
* `org.projectnessie.error.NessieConflictException: Failed to commit data. Unable to complete commit due to conflicting events` (1)
* `org.projectnessie.client.rest.NessieInternalServerException: Internal Server Error (HTTP/500): software.amazon.awssdk.servic` (few)
* No throttling (due to conditional update failures), except on l1/writes

## `-Dsim.users=5 -Dsim.commits=10000   -Dsim.branch=u5-c10000-contended-8   -Dsim.multipleBranches=false  -Dsim.prometheus=127.0.0.1:9091   -Dsim.commitLog=false   -Dhttp.maxConnections=100`

* Okay with r+w capacity=500 + l1,l2,refs=2000/2000

## `-Dsim.users=50   -Dsim.commits=10000   -Dsim.branch=u20-c10000-contended-10   -Dsim.multipleBranches=true  -Dsim.prometheus=127.0.0.1:9091   -Dsim.commitLog=false   -Dhttp.maxConnections=100`

* r+w capacity=500 + l1,l2,refs=2000/2000
* `org.projectnessie.client.rest.NessieInternalServerException: Internal Server Error (HTTP/500): software.amazon.awssdk.servic` (few)
* No throttling (due to conditional update failures), except on l1/writes

## `-Dsim.users=20   -Dsim.commits=100000   -Dsim.branch=u20-c10000-contended-12 -Dsim.multipleBranches=true  -Dsim.prometheus=127.0.0.1:9091   -Dsim.commitLog=false   -Dhttp.maxConnections=100`

* r+w capacity=500 + l1,l2,refs=2000/2000
* Works for a while (~ up to 10000 commits)
* Throttling kicks in, requests fail
* `org.projectnessie.error.NessieNotFoundException: Failed to commit data. Unable to find requested ref for commit.`

# TODOs

* Verify commit-code-path + simulate dynamodb errors, especially check for potential consistency issues
* Verify collapse-code-path + simulate dynamodb errors, especially check for potential consistency issues
* Re-think data model
  * 50 commit/s against a single branch (accesses against nessie_ref by commit + retries and collapse + retries)
  * Solution for 200 commits/second against a single branch (200 is what DynamoDB allows against a single partition key)
  * Solution for more than commits/second against a single branch
  * Collapse l1+l2+l3+commit-metadata ?
  * Full-table scan for all refs is expensive and slow - keep IDs of named references in a single partition?
  * Many tables - most need the same read/write capacities - cost
  * Lots of tables need huge r+w capacities
  * Cross-region / cross-availability-zone consistency...
    How can we guarantee that a Nessie commit including all contents (aka the data files) is consistent?
* Check for outstanding collapses (if a branch-collapse didn't succeed - delayed check)
* Have something to detect hash-collisions
* Concerning: `org.projectnessie.error.NessieNotFoundException: Failed to commit data. Unable to find requested ref for commit.` - consistency-level issue?
  --> not seen w/ billing-mode pay-per-request, so the load-failure probably happened, because the provisioned throughput was exceeded 
* Metrics for collapse-intention-log
* Prometheus Pushgateway support for Nessie (for Lambdas or auto-scaling, etc)
* No retry in commit() when specifying an expectedHash (if the Store.update doesn't succeed in the first attempt, it will never succeed)
  --> Seems, this is already handled?
* Make sure that `org.projectnessie.versioned.VersionStore.getNamedRefs` works even if a branch cannot be loaded,
  work-around corrupted branch records, but "excessively" log the issue. Otherwise Nesse will never start, because
  `org.projectnessie.server.providers.ConfigurableVersionStoreFactory.getVersionStore` cannot succeed and startup
  fails forever.

DONE
* change default billing-mode to BILLING
* Build a mock-in-memory `Store` implementation for tests
* Rate-limited benchmark
* Metrics: add distributionStatisticExpiry(1m)
* Metrics for errors (count/rate)
* Log errors to console/file (e.g. for commit operation)
* Check aws-sdk exceptions for throttling

BENCHMARKS
* provisioned R+W capacity vs. dynamic
* in-memory vs dynamo




org.projectnessie.client.rest.NessieInternalServerException: Internal Server Error (HTTP/500): 
org.projectnessie.versioned.store.StoreOperationException: Dynamo failure during save. 
java.lang.RuntimeException: 
org.projectnessie.versioned.store.StoreOperationException: Dynamo failure during save.
  at org.projectnessie.versioned.MetricsVersionStore.measure2(MetricsVersionStore.java:114)
  at org.projectnessie.versioned.MetricsVersionStore.commit(MetricsVersionStore.java:151)
  at org.projectnessie.services.rest.BaseResource.doOps(BaseResource.java:78)
  at org.projectnessie.services.rest.ContentsResource.setContents(ContentsResource.java:98)
  ... 
Caused by: org.projectnessie.versioned.store.StoreOperationException: Dynamo failure during save.
  at org.projectnessie.versioned.dynamodb.DynamoStore.save(DynamoStore.java:473)
  at org.projectnessie.versioned.store.TracingStore.save(TracingStore.java:148)
  at org.projectnessie.versioned.impl.Persistence.save(Persistence.java:82)
  at org.projectnessie.versioned.impl.InternalBranch$UpdateState.save(InternalBranch.java:349)
  at org.projectnessie.versioned.impl.InternalBranch$UpdateState.ensureAvailable(InternalBranch.java:366)
  at org.projectnessie.versioned.impl.TieredVersionStore.ensureValidL1(TieredVersionStore.java:375)
  at org.projectnessie.versioned.impl.PartialTree.lambda$getLoadChain$0(PartialTree.java:114)
  at org.projectnessie.versioned.impl.EntityLoadOps$EntityLoadOp.lambda$done$0(EntityLoadOps.java:223)
  at java.util.ArrayList.forEach(ArrayList.java:1541)
  at org.projectnessie.versioned.impl.EntityLoadOps$EntityLoadOp.done(EntityLoadOps.java:223)
  at org.projectnessie.versioned.dynamodb.DynamoStore.load(DynamoStore.java:273)
  at org.projectnessie.versioned.store.TracingStore.load(TracingStore.java:82)
  at org.projectnessie.versioned.impl.Persistence.load(Persistence.java:103)
  at org.projectnessie.versioned.impl.TieredVersionStore.commit(TieredVersionStore.java:255)
  at org.projectnessie.versioned.TracingVersionStore.commit(TracingVersionStore.java:98)
  at org.projectnessie.versioned.MetricsVersionStore.lambda$commit$3(MetricsVersionStore.java:151)
  at org.projectnessie.versioned.MetricsVersionStore.measure2(MetricsVersionStore.java:110)
  ... 43 more 
Caused by: software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException: The level of configured provisioned throughput for the table was exceeded. Consider increasing your provisioning level with the UpdateTable API. (Service: DynamoDb, Status Code: 400, Request ID: 26USQGFRKHD9RC7A6G3BODV2JJVV4KQNSO5AEMVJF66Q9ASUAAJG, Extended Request ID: null)
  at software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException$BuilderImpl.build(ProvisionedThroughputExceededException.java:122)
  ...
  at java.lang.Thread.run(Thread.java:834)
  ... 2 more 




org.projectnessie.client.rest.NessieInternalServerException: Internal Server Error (HTTP/500): java.lang.IllegalArgumentException: Did not receive any objects for table(s) [nessie_refs]. java.lang.RuntimeException: java.lang.IllegalArgumentException: Did not receive any objects for table(s) [nessie_refs].
  at org.projectnessie.versioned.MetricsVersionStore.measure2(MetricsVersionStore.java:114)
  at org.projectnessie.versioned.MetricsVersionStore.commit(MetricsVersionStore.java:151)
  at org.projectnessie.services.rest.BaseResource.doOps(BaseResource.java:78)
  at org.projectnessie.services.rest.ContentsResource.setContents(ContentsResource.java:98)
  ...
Caused by: java.lang.IllegalArgumentException: Did not receive any objects for table(s) [nessie_refs].
  at com.google.common.base.Preconditions.checkArgument(Preconditions.java:217)
  at org.projectnessie.versioned.dynamodb.DynamoStore.load(DynamoStore.java:235)
  at org.projectnessie.versioned.store.TracingStore.load(TracingStore.java:82)
  at org.projectnessie.versioned.impl.Persistence.load(Persistence.java:103)
  at org.projectnessie.versioned.impl.TieredVersionStore.commit(TieredVersionStore.java:255)
  at org.projectnessie.versioned.TracingVersionStore.commit(TracingVersionStore.java:98)
  at org.projectnessie.versioned.MetricsVersionStore.lambda$commit$3(MetricsVersionStore.java:151)
  at org.projectnessie.versioned.MetricsVersionStore.measure2(MetricsVersionStore.java:110)


org.projectnessie.client.rest.NessieInternalServerException: Internal Server Error (HTTP/500): 
java.lang.RuntimeException: Backend failure during commit: 
software.amazon.awssdk.services.dynamodb.model.DynamoDbException: Throughput exceeds the current capacity of your table or index. DynamoDB is automatically scaling your table or index so please try again shortly. If exceptions persist, check if you have a hot key: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html (Service: DynamoDb, Status Code: 400, Request ID: VP3K1VFFUQDD3J4EV89D5DEGTFVV4KQNSO5AEMVJF66Q9ASUAAJG, Extended Request ID: null) 
java.lang.RuntimeException: 
java.lang.RuntimeException: Backend failure during commit: 
software.amazon.awssdk.services.dynamodb.model.DynamoDbException: Throughput exceeds the current capacity of your table or index. DynamoDB is automatically scaling your table or index so please try again shortly. If exceptions persist, check if you have a hot key: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html (Service: DynamoDb, Status Code: 400, Request ID: VP3K1VFFUQDD3J4EV89D5DEGTFVV4KQNSO5AEMVJF66Q9ASUAAJG, Extended Request ID: null)
  at org.projectnessie.versioned.MetricsVersionStore.measure2(MetricsVersionStore.java:129)
  at org.projectnessie.versioned.MetricsVersionStore.commit(MetricsVersionStore.java:165)
  at org.projectnessie.services.rest.BaseResource.doOps(BaseResource.java:78)
  at org.projectnessie.services.rest.ContentsResource.setContents(ContentsResource.java:98)
  ...
Caused by: java.lang.RuntimeException: Backend failure during commit: software.amazon.awssdk.services.dynamodb.model.DynamoDbException: Throughput exceeds the current capacity of your table or index. DynamoDB is automatically scaling your table or index so please try again shortly. If exceptions persist, check if you have a hot key: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html (Service: DynamoDb, Status Code: 400, Request ID: VP3K1VFFUQDD3J4EV89D5DEGTFVV4KQNSO5AEMVJF66Q9ASUAAJG, Extended Request ID: null)
  at org.projectnessie.versioned.impl.TieredVersionStore.commit(TieredVersionStore.java:341)
  at org.projectnessie.versioned.TracingVersionStore.commit(TracingVersionStore.java:100)
  at org.projectnessie.versioned.MetricsVersionStore.lambda$commit$4(MetricsVersionStore.java:165)
  at org.projectnessie.versioned.MetricsVersionStore.measure2(MetricsVersionStore.java:122) ... 43 more 
Caused by: software.amazon.awssdk.services.dynamodb.model.DynamoDbException: Throughput exceeds the current capacity of your table or index. DynamoDB is automatically scaling your table or index so please try again shortly. If exceptions persist, check if you have a hot key: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html (Service: DynamoDb, Status Code: 400, Request ID: VP3K1VFFUQDD3J4EV89D5DEGTFVV4KQNSO5AEMVJF66Q9ASUAAJG, Extended Request ID: null)
  at software.amazon.awssdk.core.internal.http.CombinedResponseHandler.handleErrorResponse(CombinedResponseHandler.java:123)
  ...
  at software.amazon.awssdk.services.dynamodb.DefaultDynamoDbClient.updateItem(DefaultDynamoDbClient.java:6081)
  at org.projectnessie.versioned.dynamodb.DynamoStore.update(DynamoStore.java:510)
  at org.projectnessie.versioned.store.TracingStore.update(TracingStore.java:182)
  at org.projectnessie.versioned.impl.Persistence.update(Persistence.java:111)
  at org.projectnessie.versioned.impl.TieredVersionStore.commit(TieredVersionStore.java:319)

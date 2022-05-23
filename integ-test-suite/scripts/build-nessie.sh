#!/usr/bin/env bash
#
# Copyright (C) 2022 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

. "$(dirname "$0")"/head.in.sh
source "${DECLARES}"

echo ""
echo "Building Nessie"
echo "---------------"
echo ""

# Build Nessie base (quick build should be sufficient)
(cd "${NESSIE_DIR}" &&
  # Note: this (-Dquickly) does not build any of the Spark/Deltalake clients, which is correct,
  # because those modules must be built _after_ Iceberg, since the Nessie Spark/Deltalake modules
  # pull in the Nessie code via Iceberg artifacts.
  ${MAVEN_BINARY} \
    -Puber-jar \
    -Dquickly \
    "$@"
)

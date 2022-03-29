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
echo "Building Trino"
echo "--------------"
echo ""

if [[ -z ${nessie_nessie_version} ]] ; then
  echo "Required variable 'nessie_nessie_version' not present, need to build Nessie first."
  exit 1
fi
if [[ -z ${iceberg_iceberg_version} ]] ; then
  echo "Required variable 'iceberg_iceberg_version' not present, need to build Iceberg first."
  exit 1
fi

declare -g trino_trino_version

(cd "${TRINO_DIR}" &&
  ${MAVEN_BINARY} \
    -Ddep.nessie.version="${nessie_nessie_version}" \
    -Ddep.iceberg.version="${iceberg_iceberg_version}" \
    -am -pl ":trino-iceberg" \
    clean install -DskipTests \
    "$@"
)

trino_trino_version="$(cd "${TRINO_DIR}" &&
  # Get Trino version, must use mvn or mvnw here
  ./mvnw help:evaluate -Dexpression=project.version  -q -DforceStdout
)" || exit 1

write_declares

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
echo "Test Integrations"
echo "-----------------"
echo ""
echo "Using the following versions:"
echo "  Nessie version  : ${nessie_nessie_version}"
echo "  Iceberg version : ${iceberg_iceberg_version}"
echo "  Trino version   : ${trino_trino_version}"
echo ""

if [[ -z ${nessie_nessie_version} ]] ; then
  echo "Required variable 'iceberg_iceberg_version' not present, need to build Nessie first."
  exit 1
fi
if [[ -z ${iceberg_iceberg_version} ]] ; then
  echo "Required variable 'iceberg_iceberg_version' not present, need to build Iceberg first."
  exit 1
fi

TRINO_CONFIG=""

if [[ -n ${trino_trino_version} ]] ; then
  TRINO_CONFIG="-Dtrino.version=${trino_trino_version}"
fi

(cd "${SCRIPT_DIR}"/../tests &&
  ${MAVEN_BINARY} \
    -Dnessie.version="${nessie_nessie_version}" \
    -Diceberg.version="${iceberg_iceberg_version}" \
    ${TRINO_CONFIG} \
    -T1 \
    verify \
    "$@"
)

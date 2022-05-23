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
echo "Test Iceberg"
echo "------------"
echo ""

if [[ -z ${nessie_nessie_version} ]] ; then
  echo "Required variable 'nessie_nessie_version' not present, need to build Nessie first."
  exit 1
fi

(cd "${ICEBERG_DIR}" || exit 1
  # Tweak Iceberg's versions.props file to use "current" Nessie version
  temp_versions_props=$(mktemp)
  cp versions.props "${temp_versions_props}"
  sed 's/^org.projectnessie:*$/org.projectnessie:* = '"${nessie_nessie_version}"'/' < "${temp_versions_props}" > versions.props
  rm "${temp_versions_props}"

  ./gradlew -DflinkVersions="1.14" -DhiveVersions="3" -DsparkVersions="3.1,3.2" \
    ":iceberg-nessie:test" \
    "$@"
)

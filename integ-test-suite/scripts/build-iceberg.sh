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
echo "Building Iceberg"
echo "----------------"
echo ""

if [[ -z ${nessie_nessie_version} ]] ; then
  echo "Required variable 'nessie_nessie_version' not present, need to build Nessie first."
  exit 1
fi

# Build + Test Iceberg
(cd "${ICEBERG_DIR}" || exit 1
  # Tweak Iceberg's versions.props file to use "current" Nessie version
  temp_versions_props=$(mktemp)
  cp versions.props "${temp_versions_props}"
  sed 's/^org.projectnessie:*$/org.projectnessie:* = '"${nessie_nessie_version}"'/' < "${temp_versions_props}" > versions.props
  rm "${temp_versions_props}"

  if [[ ! -f version.txt ]] ; then
    # Automatic version detection in the Iceberg build file does not in two cases:
    # 1. when using Git worktrees (due to the Palantir plugin + JGit)
    # 2. with shallow clones (i.e. no Git tags)
    git fetch --tags --depth=1
    git for-each-ref --sort=-creatordate "refs/tags/release-base-*" --format="%(refname:short)" --count=1 | \
      sed -E 's/release-base-([0-9]+)[.]([0-9]+)[.]([0-9]+).*/\1 \2/g' | \
      awk '{print $1 "." ($2 + 1) ".0-SNAPSHOT"}' > version.txt
  fi

  ./gradlew -DflinkVersions="1.14" -DhiveVersions="3" -DsparkVersions="3.1,3.2" \
    publishApachePublicationToMavenLocal \
    "$@"
)

# Get Iceberg version + update declared variables
iceberg_iceberg_version="$(cd "${ICEBERG_DIR}" &&
  ./gradlew -q printVersion
)"

write_declares

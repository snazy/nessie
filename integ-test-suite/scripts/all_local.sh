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

declare local_skipSetup
declare local_skipBuild
declare local_skipTests
declare local_skipIntegrationTests
declare local_skipTrino
declare local_skipIceberg

while [[ $# -gt 0 ]]; do
  case $1 in
    -s|--skip-setup)
      local_skipSetup=1
      shift
      ;;
    -b|--skip-build)
      local_skipBuild=1
      shift
      ;;
    -t|--skip-tests)
      local_skipTests=1
      shift
      ;;
    -i|--skip-integration-tests)
      local_skipIntegrationTests=1
      shift
      ;;
    -T|--skip-trino)
      local_skipTrino=1
      shift
      ;;
    -I|--skip-iceberg)
      local_skipIceberg=1
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [-x|--skip-tests] [-n|--skip-build]"
      echo ""
      echo "-s|--skip-setup               Skip (re-)initialization of the project."
      echo "-b|--skip-build               Skip projects builds."
      echo "-t|--skip-tests               Skip execution of project tests."
      echo "-i|--skip-integration-tests   Skip execution of integration tests."
      echo "-I|--skip-iceberg             Skip test of Iceberg (build is mandatory)."
      echo "-T|--skip-trino               Skip build + test of Trino."
      exit 1
      ;;
    *)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

if [[ -z ${local_skipSetup} ]] ; then
  "${SCRIPT_DIR}"/init.sh
  "${SCRIPT_DIR}"/prepare_local_git.sh
  "${SCRIPT_DIR}"/setup_for_config.sh
fi

if [[ -z ${local_skipBuild} ]] ; then
  "${SCRIPT_DIR}"/build-nessie.sh
  "${SCRIPT_DIR}"/build-iceberg.sh
  if [[ -z ${local_skipTrino} ]] ; then
    "${SCRIPT_DIR}"/build-trino.sh
  fi
  "${SCRIPT_DIR}"/build-nessie-2.sh
fi

if [[ -z ${local_skipTests} ]] ; then
  "${SCRIPT_DIR}"/test-nessie.sh
  if [[ -z ${local_skipIceberg} ]] ; then
    "${SCRIPT_DIR}"/test-iceberg.sh
  fi
  if [[ -z ${local_skipTrino} ]] ; then
    "${SCRIPT_DIR}"/test-trino.sh
  fi
fi

if [[ -z ${local_skipIntegrationTests} ]] ; then
  "${SCRIPT_DIR}"/test-integrations.sh
fi

source "${DECLARES}"
echo ""
echo "Nessie Integrations Test Suite completed successfully."
echo ""
echo "Artifacts installed to local Maven repository using the following versions:"
echo "  Nessie version  : ${nessie_nessie_version}"
echo "  Iceberg version : ${iceberg_iceberg_version}"
echo "  Trino version   : ${trino_trino_version}"
echo ""
echo "Run the Maven based integrations tests using above versions via the test-integrations.sh script."

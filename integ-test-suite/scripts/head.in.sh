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

# Common functionality - nothing to configure.

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" ; pwd)
SCRIPT_BUILD_DIR="${SCRIPT_DIR}/../target"
mkdir -p "${SCRIPT_BUILD_DIR}"
DECLARES="${SCRIPT_BUILD_DIR}/.declare.in.sh"

function write_declares() {
    # Dump all variables, exclude read-only ones
    declare -p | grep -vE '^declare -[a-z]*r ' > "${DECLARES}"
}

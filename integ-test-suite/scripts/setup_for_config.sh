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

set -e

. "$(dirname "$0")"/head.in.sh
source "${DECLARES}"

# Default to the 'HEAD' config as defined in config.in.sh.
# To define a custom configuration that is not present in config.in.sh, set the following env vars:
#   config_name=custom
#   tools_custom_nessie=<see config.in.sh for the syntax>
#   tools_custom_iceberg=<see config.in.sh for the syntax>
#   tools_custom_trino=<see config.in.sh for the syntax>
[[ -z ${config_name} ]] && config_name=HEAD

tool_config_nessie="$(eval echo "\${tools_${config_name}_nessie}")"
tool_config_iceberg="$(eval echo "\${tools_${config_name}_iceberg}")"
tool_config_trino="$(eval echo "\${tools_${config_name}_trino}")"

if [[ -z ${tool_config_nessie} || -z ${tool_config_iceberg} || -z ${tool_config_trino} ]] ; then
  echo "Tools configurations not found for configuration name '${config_name}'!"
  echo "  tool_config_nessie='${tool_config_nessie}'  (via \$tools_${config_name}_nessie)"
  echo "  tool_config_iceberg='${tool_config_iceberg}'  (via \$tools_${config_name}_iceberg)"
  echo "  tool_config_trino='${tool_config_trino}'  (via \$tools_${config_name}_trino)"
  exit 1
fi

echo "Configuration for Nessie source tree: ${tool_config_nessie}"
echo "Configuration for Iceberg source tree: ${tool_config_iceberg}"
echo "Configuration for Trino source tree: ${tool_config_trino}"

function git_get_remote_for_url() {
  git remote -v | grep "${1}" | grep -w fetch | awk '{print $1}'
}

function git_clone() {
  local dir="$1"
  local url_at_branch="$2"

  repo_url="$(echo "${url_at_branch}" | cut -d@ -f1)"
  # Strip trailing '.git'
  repo_url="${repo_url%.git}"
  branch="$(echo "${url_at_branch}" | cut -d@ -f2)"

  echo "Initializing Git repo ..."
  git init ${dir}
  (cd "${dir}" || exit 1
    echo "Adding remopte origin for ${repo_url} ..."
    git remote add origin "${repo_url}"
    git config --local gc.auto 0
    echo "Fetching from origin refs/heads/${branch}:${branch} ..."
    git -c protocol.version=2 fetch --no-tags --prune --no-recurse-submodules origin "refs/heads/${branch}"
    echo "Checkout ${branch} ..."
    git checkout --force -B "${branch}" "origin/${branch}"
    echo "Done."
  )
}

function git_fetch() {
  local url_at_branch="$1"

  local repo_url
  local branch
  local remote_name
  local local_branch

  repo_url="$(echo "${url_at_branch}" | cut -d@ -f1)"
  # Strip trailing '.git'
  repo_url="${repo_url%.git}"
  branch="$(echo "${url_at_branch}" | cut -d@ -f2)"

  remote_name="$(git_get_remote_for_url "${repo_url}.git")"
  if [[ -z ${remote_name} ]] ; then
    remote_name="$(git_get_remote_for_url "${repo_url}")"
  fi
  if [[ -z ${remote_name} ]] ; then
    remote_name="$(echo "${repo_url}" | cut -d/ -f4)"
    git remote add "${remote_name}" "${repo_url}" > /dev/null || exit 1
  fi

  # If the the branch is available locally, prefer the local branch
  local_branch=$(git for-each-ref --format='%(refname:short) %(upstream:short)' refs/heads | grep " ${remote_name}/${branch}" | cut -d\  -f1)
  if [[ -z ${local_branch} ]] ; then
    git fetch --quiet "${remote_name}" "${branch}" > /dev/null || exit 1
    echo "${remote_name}/${branch}"
  else
    echo "${remote_name}/${branch} ${local_branch}"
  fi
}

function apply_patch() {
  git_branch="$1"
  git_base_branches="$2"

  echo "Applying the following changes to nearest common ancestor of \"${git_branch}\" and \"${git_base_branches}\":"
  merge_base="$(git merge-base "${git_branch}" ${git_base_branches})"
  echo ""
  echo "Git log of the applied diff (common ancestor is ${merge_base} - patches up to ${git_branch})"
  echo "--------------------------------------------------------------------------------------------"
  git log --oneline "${merge_base}..${git_branch}" | cat
  echo ""
  echo "... applying changes via git-diff + git-apply ..."
  git diff "${merge_base}" "${git_branch}" | git apply --allow-empty -
  echo "... done."
}

function configure_tool() {
  local tool_name="$1"
  local dir="$2"
  local cfg="$3"
  local main_repo_branch
  local main_remote_local
  local patch_repo_branch
  local patch_remote_local
  local patch_ref

  echo ""
  echo "Configuring ${tool_name} source tree in ${dir}"
  echo "==========================================================================================="
  echo ""

  if [[ ${cfg} =~ .+[+].+ ]] ; then
    main_repo_branch="$(echo "$cfg" | cut -d+ -f1)"
    echo "Main repository and branch: ${main_repo_branch}"
      patch_repo_branch="$(echo "$cfg" | cut -d+ -f2)"
      echo "Patch repository and branch: ${patch_repo_branch}"
  else
    main_repo_branch="${cfg}"
    echo "Main repository and branch: ${cfg}"
  fi

  if [[ ! -d ${dir} ]] ; then
    git_clone "${dir}" "${main_repo_branch}"
  fi

  (cd "${dir}" || exit 1
    if [[ ${cfg} =~ .+[+].+ ]] ; then
      main_remote_local="$(git_fetch "${main_repo_branch}")" || exit 1
      patch_remote_local="$(git_fetch "${patch_repo_branch}")" || exit 1
      patch_ref="$(echo "${patch_remote_local}" | cut -d\  -f2)"

      apply_patch "${patch_ref}" "${main_remote_local}"
    else
      main_remote_local="$(git_fetch "${cfg}")" || exit 1
      echo "(No patch)"
    fi
  )
}

configure_tool "Nessie" "${NESSIE_DIR}" "${tool_config_nessie}"
configure_tool "Iceberg" "${ICEBERG_DIR}" "${tool_config_iceberg}"
configure_tool "Trino" "${TRINO_DIR}" "${tool_config_trino}"

write_declares

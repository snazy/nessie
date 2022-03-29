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

# Syntax for git-source + optional patch:
#   Key name:
#     'tools_' + config_name + '_' product_name
#   Value:
#     git_remote_url '@' branch_name [ '+' git_remote_url_for_patch '@' branch_name_for_patch ]

tools_HEAD_nessie=https://github.com/projectnessie/nessie.git@main+https://github.com/snazy/nessie@integ-bump/iceberg
tools_HEAD_iceberg=https://github.com/apache/iceberg@master
tools_HEAD_trino=https://github.com/snazy/trino@add-nessie-support+https://github.com/snazy/trino.git@iceberg-0.14

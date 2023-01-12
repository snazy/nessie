/*
 * Copyright (C) 2023 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.projectnessie.nessie.ui

import js.uri.encodeURIComponent
import kotlin.js.Promise
import kotlinx.browser.window
import tanstack.query.core.QueryKey
import tanstack.react.query.useQuery
import web.location.Location
import web.url.URLSearchParams

private var nessieApiUri = "/api/v2/"

fun initAppRootUrlFromLocation(location: Location) {
  val search = location.search
  if (search.length > 1) {
    val params = URLSearchParams(location.search.substring(1))
    if (params.has("uri")) {
      nessieApiUri = params["uri"]!!
    }
  }
  console.log("Nessie API URI: ", nessieApiUri)
}

external interface NessieConfiguration {
  var defaultBranch: String
  var maxSupportedApiVersion: Int?
}

external interface NessieCommitMeta {
  var hash: String?
  var message: String?
  var committer: String?
  var allAuthors: Array<String>?
  var allSignedOffBy: Array<String>?
  var commitTime: String?
  var authorTime: String?
  var allProperties: Map<String, Array<String>>?
  var parentCommitHashes: Array<String>?
}

external interface NessieReferenceMetadata {
  var numCommitsAhead: Int?
  var numCommitsBehind: Int?
  var commitMetaOfHEAD: NessieCommitMeta?
  var commonAncestorHash: String?
  var numTotalCommits: Long?
}

external interface NessieReference {
  var type: String
  var name: String
  var hash: String?
  var metadata: NessieReferenceMetadata?
}

external interface NessiePaginatedResponse {
  var hasMore: Boolean
  var token: String?
}

external interface NessieReferencesResponse : NessiePaginatedResponse {
  var references: Array<NessieReference>
}

external interface NessieEntry {
  var type: String
  var name: NessieContentKey
  var contentId: String?
  var content: NessieContent?
}

external interface NessieEntriesResponse : NessiePaginatedResponse {
  var entries: Array<NessieEntry>
  var effectiveReference: NessieReference?
}

external interface NessieContent {
  var type: String
  var id: String?

  var metadataLocation: String?
  var snapshotId: Long?
  var schemaId: Int?
  var specId: Int?
  var sortOrderId: Int?
  var versionId: Int?
  var sqlText: String?
  var dialect: String?
}

external interface NessieOperation {
  var key: NessieContentKey
  var content: NessieContent
  var expectedContent: NessieContent?
}

external interface NessieLogEntry {
  var commitMeta: NessieCommitMeta
  var parentCommitHash: String
  var operations: NessieOperation
}

external interface NessieLogResponse : NessiePaginatedResponse {
  var logEntries: Array<NessieLogEntry>
  var effectiveReference: NessieReference?
}

external interface NessieContentKey {
  var elements: Array<String>
}

fun contentKeyAsString(key: NessieContentKey): String = key.elements.joinToString(".")

private val queryKeyConfiguration = QueryKey<QueryKey>("nessie-configuration")

fun useNessieConfiguration(): NessieConfiguration? {
  val result =
    useQuery<NessieConfiguration, Error, NessieConfiguration, QueryKey>(
      queryKey = queryKeyConfiguration,
      queryFn = { getNessieConfiguration() }
    )
  return result.data
}

private fun getNessieConfiguration(): Promise<NessieConfiguration> =
  window
    .fetch(nessieApiUrl("config"))
    .then { it.json() }
    .then { it.unsafeCast<NessieConfiguration>() }

private val queryKeyReferences = QueryKey<QueryKey>("nessie-references")

fun useReferences(
  fetchOption: String? = null,
  filter: String? = null,
  maxRecords: Int? = null,
  pageToken: String? = null
): NessieReferencesResponse? {
  val keys =
    queryKeys(
      "fetchOption" to fetchOption,
      "filter" to filter,
      "maxRecords" to maxRecords,
      "pageToken" to pageToken
    )
  val queryKey = QueryKey<QueryKey>(queryKeyReferences, *keys)
  val result =
    useQuery<NessieReferencesResponse, Error, NessieReferencesResponse, QueryKey>(
      queryKey = queryKey,
      queryFn = { getReferences(keys) }
    )
  return result.data
}

private fun getReferences(queryParams: Array<String>): Promise<NessieReferencesResponse> =
  window
    .fetch(nessieApiUrl("trees", queryParams))
    .then { it.json() }
    .then { it.unsafeCast<NessieReferencesResponse>() }

private val queryKeyEntries = QueryKey<QueryKey>("nessie-entries")

fun useEntries(
  ref: String,
  withContent: Boolean? = null,
  filter: String? = null,
  maxRecords: Int? = null,
  pageToken: String? = null
): NessieEntriesResponse? {
  val keys =
    queryKeys(
      "withContent" to withContent,
      "filter" to filter,
      "maxRecords" to maxRecords,
      "pageToken" to pageToken
    )
  val queryKey = QueryKey<QueryKey>(queryKeyEntries, ref, *keys)
  val result =
    useQuery<NessieEntriesResponse, Error, NessieEntriesResponse, QueryKey>(
      queryKey = queryKey,
      queryFn = { getEntries(ref, keys) }
    )
  return result.data
}

private fun getEntries(ref: String, queryParams: Array<String>): Promise<NessieEntriesResponse> =
  window
    .fetch(nessieApiUrl("trees/${ref}/entries", queryParams))
    .then { it.json() }
    .then { it.unsafeCast<NessieEntriesResponse>() }

private val queryKeyLog = QueryKey<QueryKey>("nessie-log")

fun useCommitLog(
  ref: String,
  fetchOption: String? = null,
  filter: String? = null,
  maxRecords: Int? = null,
  pageToken: String? = null
): NessieLogResponse? {
  val keys =
    queryKeys(
      "fetch" to fetchOption,
      "filter" to filter,
      "max-records" to maxRecords,
      "page-token" to pageToken
    )
  val queryKey = QueryKey<QueryKey>(queryKeyLog, ref, *keys)
  val result =
    useQuery<NessieLogResponse, Error, NessieLogResponse, QueryKey>(
      queryKey = queryKey,
      queryFn = { getCommitLog(ref, keys) }
    )
  return result.data
}

private fun getCommitLog(ref: String, queryParams: Array<String>): Promise<NessieLogResponse> =
  window
    .fetch(nessieApiUrl("trees/${ref}/history", queryParams))
    .then { it.json() }
    .then { it.unsafeCast<NessieLogResponse>() }

private fun queryKeys(vararg keys: Pair<String, Any?>): Array<String> =
  keys
    .filter { k -> k.second != null }
    .map { k -> "${encodeURIComponent(k.first)}=${encodeURIComponent(k.second.toString())}" }
    .toTypedArray()

private fun nessieApiUrl(path: String, queryParams: Array<String> = emptyArray()): String =
  "${nessieApiUri}$path${if (queryParams.isNotEmpty()) queryParams.joinToString("&", "?") else ""}"

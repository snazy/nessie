/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.api.v1.http;

import com.fasterxml.jackson.annotation.JsonView;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.projectnessie.api.v1.DiffApi;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.ser.Views;

@Tag(name = "v1")
@Consumes(MediaType.APPLICATION_JSON)
@jakarta.ws.rs.Consumes(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
@Path("v1/diffs")
@jakarta.ws.rs.Path("v1/diffs")
public interface HttpDiffApi extends DiffApi {

  @GET
  @jakarta.ws.rs.GET
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Path("{fromRefWithHash}...{toRefWithHash}")
  @jakarta.ws.rs.Path("{fromRefWithHash}...{toRefWithHash}")
  @Operation(
      summary = "Get a diff for two given references",
      description =
          "The URL pattern is basically 'from' and 'to' separated by '...' (three dots). "
              + "'from' and 'to' must start with a reference name, optionally followed by hash on "
              + "that reference, the hash prefixed with the'*' character.\n"
              + "\n"
              + "Examples: \n"
              + "  diffs/main...myBranch\n"
              + "  diffs/main...myBranch\\*1234567890123456\n"
              + "  diffs/main\\*1234567890123456...myBranch\n"
              + "  diffs/main\\*1234567890123456...myBranch\\*1234567890123456\n")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Returned diff for the given references.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = {
                  @ExampleObject(ref = "diffResponse"),
                },
                schema = @Schema(implementation = DiffResponse.class))),
    @APIResponse(responseCode = "400", description = "Invalid input, fromRef/toRef name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "403", description = "Not allowed to view the given fromRef/toRef"),
    @APIResponse(responseCode = "404", description = "fromRef/toRef not found"),
  })
  @JsonView(Views.V1.class)
  @Override
  DiffResponse getDiff(
      @Parameter(
              description = "The 'from' reference (and optional hash) to start the diff from",
              examples = {@ExampleObject(ref = "ref"), @ExampleObject(ref = "refForDiffWithHash")})
          @PathParam("fromRefWithHash")
          @jakarta.ws.rs.PathParam("fromRefWithHash")
          String fromRefWithHash,
      @Parameter(
              description = "The 'to' reference (and optional hash) to end the diff at.",
              examples = {@ExampleObject(ref = "ref"), @ExampleObject(ref = "refForDiffWithHash")})
          @PathParam("toRefWithHash")
          @jakarta.ws.rs.PathParam("toRefWithHash")
          String toRefWithHash)
      throws NessieNotFoundException;
}

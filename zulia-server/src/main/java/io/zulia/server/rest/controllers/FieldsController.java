package io.zulia.server.rest.controllers;

import io.micronaut.core.annotation.Blocking;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.hateoas.JsonError;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.zulia.ZuliaRESTConstants;
import io.zulia.rest.dto.FieldsDTO;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;

import static io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */

@Controller
@Tag(name = "Fields")
@ApiResponses({ @ApiResponse(responseCode = "400", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "404", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "500", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "503", content = { @Content(schema = @Schema(implementation = JsonError.class)) }) })
@ExecuteOn(TaskExecutors.VIRTUAL)
public class FieldsController {

	@Get(ZuliaRESTConstants.FIELDS_URL + "/{indexName}")
	@Produces(ZuliaRESTConstants.UTF8_JSON)
	@Operation(summary = "Get field names", description = "Returns all field names that exist in the specified index")
	public FieldsDTO getFields(
			@Parameter(description = "Index name") String indexName,
			@Parameter(description = "If true, force a commit before reading to ensure latest fields are visible") @Nullable @QueryValue(ZuliaRESTConstants.REALTIME) final Boolean realtime) throws Exception {
		return runGetFields(indexName, realtime);
	}

	@Get(ZuliaRESTConstants.FIELDS_URL)
	@Produces(ZuliaRESTConstants.UTF8_JSON)
	@Operation(summary = "Get field names (legacy)", description = "Returns field names using query parameter. Prefer /fields/{indexName} instead.", deprecated = true)
	public FieldsDTO getFieldsLegacy(
			@Parameter(description = "Index name") @QueryValue(ZuliaRESTConstants.INDEX) final String indexName,
			@Parameter(description = "If true, force a commit before reading to ensure latest fields are visible") @Nullable @QueryValue(ZuliaRESTConstants.REALTIME) final Boolean realtime) throws Exception {
		return runGetFields(indexName, realtime);
	}

	private static FieldsDTO runGetFields(String indexName, Boolean realtime) throws Exception {
		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();
		GetFieldNamesRequest.Builder fieldNamesRequestBuilder = GetFieldNamesRequest.newBuilder().setIndexName(indexName);
		if (realtime != null) {
			fieldNamesRequestBuilder.setRealtime(realtime);
		}
		GetFieldNamesRequest fieldNamesRequest = fieldNamesRequestBuilder.build();
		GetFieldNamesResponse fieldNamesResponse = indexManager.getFieldNames(fieldNamesRequest);

		FieldsDTO fieldsDTO = new FieldsDTO();
		fieldsDTO.setIndex(indexName);
		fieldsDTO.setFields(fieldNamesResponse.getFieldNameList());
		return fieldsDTO;
	}

}

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
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
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
@ApiResponses({ @ApiResponse(responseCode = "400", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "404", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "500", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "503", content = { @Content(schema = @Schema(implementation = JsonError.class)) }) })
@ExecuteOn(TaskExecutors.VIRTUAL)
public class FieldsController {

	@Get(ZuliaRESTConstants.FIELDS_URL)
	@Produces(ZuliaRESTConstants.UTF8_JSON)
	public FieldsDTO getFields(@QueryValue(ZuliaRESTConstants.INDEX) final String indexName,
			@Nullable @QueryValue(ZuliaRESTConstants.REALTIME) final Boolean realtime) throws Exception {
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

package io.zulia.server.rest.controllers;

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
import io.zulia.server.exceptions.DocumentDoesNotExistException;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import io.zulia.util.ResultHelper;
import org.bson.Document;

import static io.zulia.message.ZuliaServiceOuterClass.FetchRequest;
import static io.zulia.message.ZuliaServiceOuterClass.FetchResponse;

@Controller
@ApiResponses({ @ApiResponse(responseCode = "400", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "404", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "500", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "503", content = { @Content(schema = @Schema(implementation = JsonError.class)) }) })
public class FetchController {

	@ExecuteOn(TaskExecutors.BLOCKING)
	@Get(ZuliaRESTConstants.FETCH_URL)
	@Produces(ZuliaRESTConstants.UTF8_JSON)
	public String fetch(@QueryValue(ZuliaRESTConstants.ID) String uniqueId, @QueryValue(ZuliaRESTConstants.INDEX) String indexName) throws Exception {
		return runFetch(uniqueId, indexName);
	}

	@ExecuteOn(TaskExecutors.BLOCKING)
	@Get(ZuliaRESTConstants.FETCH_URL + "/{indexName}/{uniqueId}")
	@Produces(ZuliaRESTConstants.UTF8_JSON)
	public String fetchPath(String uniqueId, String indexName) throws Exception {
		return runFetch(uniqueId, indexName);
	}

	private static String runFetch(String uniqueId, String indexName) throws Exception {
		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		FetchRequest.Builder fetchRequest = FetchRequest.newBuilder();
		fetchRequest.setIndexName(indexName);
		fetchRequest.setUniqueId(uniqueId);

		FetchResponse fetchResponse = indexManager.fetch(fetchRequest.build());

		if (fetchResponse.hasResultDocument()) {
			Document document = ResultHelper.getDocumentFromResultDocument(fetchResponse.getResultDocument());
			if (document != null) {
				return document.toJson();
			}
		}
		throw new DocumentDoesNotExistException(uniqueId, indexName);
	}

}
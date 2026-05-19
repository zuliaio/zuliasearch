package io.zulia.server.rest.controllers;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Produces;
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
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.index.replication.ReplicationShardState;
import io.zulia.server.node.ZuliaNode;

import java.util.List;

@Controller
@Tag(name = "Replication")
@ApiResponses({ @ApiResponse(responseCode = "400", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "404", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "500", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "503", content = { @Content(schema = @Schema(implementation = JsonError.class)) }) })
@ExecuteOn(TaskExecutors.VIRTUAL)
public class ReplicationController {

	private final ZuliaNode zuliaNode;

	public ReplicationController(ZuliaNode zuliaNode) {
		this.zuliaNode = zuliaNode;
	}

	@Get(ZuliaRESTConstants.REPLICATION_URL + "/{indexName}")
	@Produces(ZuliaRESTConstants.UTF8_JSON)
	@Operation(summary = "Get replication state",
			description = "Per-shard and per-replica replication progress for the given index. Only shards whose primary lives on this node have populated state.")
	public List<ReplicationShardState> getReplicationState(@Parameter(description = "Index name") @PathVariable String indexName) throws Exception {
		ZuliaIndexManager indexManager = zuliaNode.getIndexManager();
		return indexManager.getReplicationState(indexName);
	}
}

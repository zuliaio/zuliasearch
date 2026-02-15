package io.zulia.server.rest.controllers;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.hateoas.JsonError;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.zulia.ZuliaRESTConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.NodeStats;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import io.zulia.util.ZuliaVersion;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
@Controller
@Tag(name = "Stats")
@ApiResponses({ @ApiResponse(responseCode = "500", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "503", content = { @Content(schema = @Schema(implementation = JsonError.class)) }) })
@ExecuteOn(TaskExecutors.VIRTUAL)
public class StatsController {

	private static final int MB = 1024 * 1024;

	@Get(ZuliaRESTConstants.STATS_URL)
	@Produces(ZuliaRESTConstants.UTF8_JSON)
	@Operation(summary = "Get node statistics", description = "Returns JVM memory usage, disk space, Zulia version, and per-index statistics for the current node")
	public NodeStats getStats() throws IOException {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		Runtime runtime = Runtime.getRuntime();

		File dataDir = new File(ZuliaNodeProvider.getZuliaNode().getZuliaConfig().getDataPath());
		double freeDataDirSpaceGB = dataDir.getFreeSpace() / (1024.0 * 1024 * 1024);
		double totalDataDirSpaceGB = dataDir.getTotalSpace() / (1024.0 * 1024 * 1024);
		double usedDataDirSpaceGB = totalDataDirSpaceGB - freeDataDirSpaceGB;

		NodeStats.Builder nodeStats = NodeStats.newBuilder();

		nodeStats.setJvmUsedMemoryMB((runtime.totalMemory() - runtime.freeMemory()) / MB);
		nodeStats.setJvmFreeMemoryMB((runtime.freeMemory()) / MB);
		nodeStats.setJvmTotalMemoryMB((runtime.totalMemory()) / MB);
		nodeStats.setJvmMaxMemoryMB((runtime.maxMemory()) / MB);
		nodeStats.setFreeDataDirSpaceGB(freeDataDirSpaceGB);
		nodeStats.setTotalDataDirSpaceGB(freeDataDirSpaceGB);
		nodeStats.setUsedDataDirSpaceGB(usedDataDirSpaceGB);
		nodeStats.setZuliaVersion(ZuliaVersion.getVersion());

		List<ZuliaBase.IndexStats> stats = indexManager.getIndexStats();
		nodeStats.addAllIndexStat(stats);

		return nodeStats.build();

	}
}

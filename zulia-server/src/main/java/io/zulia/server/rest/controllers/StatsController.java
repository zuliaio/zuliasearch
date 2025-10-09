package io.zulia.server.rest.controllers;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.zulia.ZuliaRESTConstants;
import io.zulia.rest.dto.StatsDTO;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import io.zulia.util.ZuliaVersion;

import java.io.File;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
@Controller
@ExecuteOn(TaskExecutors.VIRTUAL)
public class StatsController {

	private static final int MB = 1024 * 1024;

	@Get(ZuliaRESTConstants.STATS_URL)
	@Produces(ZuliaRESTConstants.UTF8_JSON)
	public StatsDTO getStats() {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		Runtime runtime = Runtime.getRuntime();

		File dataDir = new File(ZuliaNodeProvider.getZuliaNode().getZuliaConfig().getDataPath());
		double freeDataDirSpaceGB = dataDir.getFreeSpace() / (1024.0 * 1024 * 1024);
		double totalDataDirSpaceGB = dataDir.getTotalSpace() / (1024.0 * 1024 * 1024);
		double usedDataDirSpaceGB = totalDataDirSpaceGB - freeDataDirSpaceGB;

		StatsDTO statsDTO = new StatsDTO();

		statsDTO.setJvmUsedMemoryMB((runtime.totalMemory() - runtime.freeMemory()) / MB);
		statsDTO.setJvmFreeMemoryMB((runtime.freeMemory()) / MB);
		statsDTO.setJvmTotalMemoryMB((runtime.totalMemory()) / MB);
		statsDTO.setJvmMaxMemoryMB((runtime.maxMemory()) / MB);
		statsDTO.setFreeDataDirSpaceGB(freeDataDirSpaceGB);
		statsDTO.setTotalDataDirSpaceGB(freeDataDirSpaceGB);
		statsDTO.setUsedDataDirSpaceGB(usedDataDirSpaceGB);
		statsDTO.setZuliaVersion(ZuliaVersion.getVersion());

		//TODO use this
		indexManager.getStats();

		return statsDTO;

	}
}

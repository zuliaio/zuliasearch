package io.zulia.server.rest.controllers;

import com.cedarsoftware.util.io.JsonWriter;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.zulia.ZuliaConstants;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import io.zulia.util.ZuliaVersion;
import org.bson.Document;

import java.io.File;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Controller(ZuliaConstants.STATS_URL)
public class StatsController {

	private static final int MB = 1024 * 1024;

	@Get
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
	public HttpResponse<String> get(@QueryValue(value = ZuliaConstants.PRETTY, defaultValue = "true") Boolean pretty) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		try {

			Document mongoDocument = new Document();

			Runtime runtime = Runtime.getRuntime();

			File dataDir = new File(ZuliaNodeProvider.getZuliaNode().getZuliaConfig().getDataPath());
			double freeDataDirSpaceGB = dataDir.getFreeSpace() / (1024.0 * 1024 * 1024);
			double totalDataDirSpaceGB = dataDir.getTotalSpace() / (1024.0 * 1024 * 1024);
			double usedDataDirSpaceGB = totalDataDirSpaceGB - freeDataDirSpaceGB;

			mongoDocument.put("jvmUsedMemoryMB", (runtime.totalMemory() - runtime.freeMemory()) / MB);
			mongoDocument.put("jvmFreeMemoryMB", runtime.freeMemory() / MB);
			mongoDocument.put("jvmTotalMemoryMB", runtime.totalMemory() / MB);
			mongoDocument.put("jvmMaxMemoryMB", runtime.maxMemory() / MB);
			mongoDocument.put("freeDataDirSpaceGB", freeDataDirSpaceGB);
			mongoDocument.put("totalDataDirSpaceGB", totalDataDirSpaceGB);
			mongoDocument.put("usedDataDirSpaceGB", usedDataDirSpaceGB);
			mongoDocument.put("zuliaVersion", ZuliaVersion.getVersion());

			indexManager.getStats();

			String docString = mongoDocument.toJson();

			if (pretty) {
				docString = JsonWriter.formatJson(docString);
			}

			return HttpResponse.ok(docString).status(ZuliaConstants.SUCCESS);

		}
		catch (Exception e) {
			return HttpResponse.serverError("Failed to get cluster membership: " + e.getMessage()).status(ZuliaConstants.INTERNAL_ERROR);
		}

	}

}

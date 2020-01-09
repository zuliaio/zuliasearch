package io.zulia.server.rest.controllers;

import com.cedarsoftware.util.io.JsonWriter;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.zulia.ZuliaConstants;
import org.bson.Document;

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

		try {

			Document mongoDocument = new Document();

			Runtime runtime = Runtime.getRuntime();

			mongoDocument.put("jvmUsedMemoryMB", (runtime.totalMemory() - runtime.freeMemory()) / MB);
			mongoDocument.put("jvmFreeMemoryMB", runtime.freeMemory() / MB);
			mongoDocument.put("jvmTotalMemoryMB", runtime.totalMemory() / MB);
			mongoDocument.put("jvmMaxMemoryMB", runtime.maxMemory() / MB);

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

package io.zulia.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import io.zulia.ZuliaConstants;
import io.zulia.server.index.ZuliaIndexManager;
import org.bson.Document;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Path(ZuliaConstants.STATS_URL)
public class StatsResource {

	private static final int MB = 1024 * 1024;

	private ZuliaIndexManager indexManager;

	public StatsResource(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
	public Response get(@Context Response response, @QueryParam(ZuliaConstants.PRETTY) boolean pretty) {

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

			return Response.status(ZuliaConstants.SUCCESS).entity(docString).build();

		}
		catch (Exception e) {
			return Response.status(ZuliaConstants.INTERNAL_ERROR).entity("Failed to get cluster membership: " + e.getMessage()).build();
		}

	}

}

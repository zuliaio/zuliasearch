package io.zulia.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import com.mongodb.util.JSONSerializers;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.util.ZuliaConstants;
import org.bson.Document;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static io.zulia.message.ZuliaServiceOuterClass.GetIndexesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetIndexesResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Path(ZuliaConstants.INDEXES_URL)
public class IndexesResource {

	private ZuliaIndexManager indexManager;

	public IndexesResource(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
	public Response get(@Context Response response, @QueryParam(ZuliaConstants.PRETTY) boolean pretty) {

		try {
			GetIndexesResponse getIndexesResponse = indexManager.getIndexes(GetIndexesRequest.newBuilder().build());

			Document mongoDocument = new org.bson.Document();
			mongoDocument.put("indexes", getIndexesResponse.getIndexNameList());
			String docString = JSONSerializers.getStrict().serialize(mongoDocument);

			if (pretty) {
				docString = JsonWriter.formatJson(docString);
			}

			return Response.status(ZuliaConstants.SUCCESS).entity(docString).build();

		}
		catch (Exception e) {
			return Response.status(ZuliaConstants.INTERNAL_ERROR).entity("Failed to get index names: " + e.getMessage()).build();
		}

	}

}

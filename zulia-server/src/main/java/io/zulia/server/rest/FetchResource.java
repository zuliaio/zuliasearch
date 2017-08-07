package io.zulia.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.util.ResultHelper;
import io.zulia.util.ZuliaConstants;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static io.zulia.message.ZuliaServiceOuterClass.FetchRequest;
import static io.zulia.message.ZuliaServiceOuterClass.FetchResponse;

@Path(ZuliaConstants.FETCH_URL)
public class FetchResource {

	private ZuliaIndexManager indexManager;

	public FetchResource(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
	public Response get(@Context Response response, @QueryParam(ZuliaConstants.ID) final String uniqueId,
			@QueryParam(ZuliaConstants.INDEX) final String indexName, @QueryParam(ZuliaConstants.PRETTY) boolean pretty) {

		FetchRequest.Builder fetchRequest = FetchRequest.newBuilder();
		fetchRequest.setIndexName(indexName);
		fetchRequest.setUniqueId(uniqueId);

		FetchResponse fetchResponse;

		try {
			fetchResponse = indexManager.fetch(fetchRequest.build());

			if (fetchResponse.hasResultDocument()) {
				Document document = ResultHelper.getDocumentFromResultDocument(fetchResponse.getResultDocument());
				if (document != null) {
					String docString;
					if (pretty) {
						docString = document.toJson(JsonWriterSettings.builder().indent(true).build());
					}
					else {
						docString = document.toJson();
					}

					if (pretty) {
						docString = JsonWriter.formatJson(docString);
					}

					return Response.status(ZuliaConstants.SUCCESS).entity(docString).build();
				}

				return Response.status(ZuliaConstants.NOT_FOUND).entity("Failed to fetch uniqueId <" + uniqueId + "> for index <" + indexName + ">").build();
			}
			else {
				return Response.status(ZuliaConstants.NOT_FOUND).entity("Failed to fetch uniqueId <" + uniqueId + "> for index <" + indexName + ">").build();
			}

		}
		catch (Exception e) {
			return Response.status(ZuliaConstants.INTERNAL_ERROR)
					.entity("Failed to fetch uniqueId <" + uniqueId + "> for index <" + indexName + ">: " + e.getMessage()).build();
		}

	}

}
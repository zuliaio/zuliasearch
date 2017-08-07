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

import static io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Path(ZuliaConstants.FIELDS_URL)
public class FieldsResource {

	private ZuliaIndexManager indexManager;

	public FieldsResource(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
	public Response get(@Context Response response, @QueryParam(ZuliaConstants.INDEX) final String indexName,
			@QueryParam(ZuliaConstants.PRETTY) boolean pretty) {

		if (indexName != null) {

			GetFieldNamesRequest fieldNamesRequest = GetFieldNamesRequest.newBuilder().setIndexName(indexName).build();

			GetFieldNamesResponse fieldNamesResponse;

			try {
				fieldNamesResponse = indexManager.getFieldNames(fieldNamesRequest);

				Document mongoDocument = new Document();
				mongoDocument.put("index", indexName);
				mongoDocument.put("fields", fieldNamesResponse.getFieldNameList());

				String docString = mongoDocument.toJson();

				if (pretty) {
					docString = JsonWriter.formatJson(docString);
				}

				return Response.status(ZuliaConstants.SUCCESS).entity(docString).build();

			}
			catch (Exception e) {
				return Response.status(ZuliaConstants.INTERNAL_ERROR).entity("Failed to fetch fields for index <" + indexName + ">: " + e.getMessage()).build();
			}
		}
		else {
			return Response.status(ZuliaConstants.INTERNAL_ERROR).entity("No index defined").build();
		}

	}

}

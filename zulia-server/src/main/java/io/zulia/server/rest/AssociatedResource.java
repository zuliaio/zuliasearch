package io.zulia.server.rest;

import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.util.StreamHelper;
import io.zulia.util.ZuliaConstants;
import org.bson.Document;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Path(ZuliaConstants.ASSOCIATED_DOCUMENTS_URL)
public class AssociatedResource {

	private final static Logger LOG = Logger.getLogger(AssociatedResource.class.getSimpleName());

	private ZuliaIndexManager indexManager;

	public AssociatedResource(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_OCTET_STREAM })
	public Response get(@Context Response response, @QueryParam(ZuliaConstants.ID) final String uniqueId,
			@QueryParam(ZuliaConstants.FILE_NAME) final String fileName, @QueryParam(ZuliaConstants.INDEX) final String indexName) {

		StreamingOutput stream = output -> {
			if (uniqueId != null && fileName != null && indexName != null) {
				InputStream is = indexManager.getAssociatedDocumentStream(indexName, uniqueId, fileName);
				if (is != null) {
					StreamHelper.copyStream(is, output);

				}
				else {
					throw new WebApplicationException("Cannot find associated document with uniqueId <" + uniqueId + "> with fileName <" + fileName + ">",
							ZuliaConstants.NOT_FOUND);
				}
			}
			else {
				throw new WebApplicationException(ZuliaConstants.ID + " and " + ZuliaConstants.FILE_NAME + " are required", ZuliaConstants.BAD_REQUEST);
			}
		};

		return Response.ok(stream).header("content-disposition", "attachment; filename = " + fileName).build();

	}

	@POST
	@Produces({ MediaType.TEXT_XML })
	public Response post(@QueryParam(ZuliaConstants.ID) String uniqueId, @QueryParam(ZuliaConstants.FILE_NAME) String fileName,
			@QueryParam(ZuliaConstants.INDEX) String indexName, @QueryParam(ZuliaConstants.COMPRESSED) Boolean compressed,
			@QueryParam(ZuliaConstants.META) List<String> meta, InputStream is) {
		if (uniqueId != null && fileName != null && indexName != null) {

			HashMap<String, String> metaMap = new HashMap<>();
			if (meta != null) {
				for (String m : meta) {
					int colonIndex = m.indexOf(":");
					if (colonIndex != -1) {
						String key = m.substring(0, colonIndex);
						String value = m.substring(colonIndex + 1).trim();
						metaMap.put(key, value);
					}
					else {
						throw new WebApplicationException("Meta must be in the form key:value");
					}
				}
			}

			try {

				if (compressed == null) {
					compressed = false;
				}

				indexManager.storeAssociatedDocument(indexName, uniqueId, fileName, is, compressed, metaMap);

				return Response.status(ZuliaConstants.SUCCESS)
						.entity("Stored associated document with uniqueId <" + uniqueId + "> and fileName <" + fileName + ">").build();
			}
			catch (Exception e) {
				LOG.log(Level.SEVERE, e.getMessage(), e);
				return Response.status(ZuliaConstants.INTERNAL_ERROR).entity(e.getMessage()).build();
			}
		}
		else {
			throw new WebApplicationException(ZuliaConstants.ID + " and " + ZuliaConstants.FILE_NAME + " are required", ZuliaConstants.BAD_REQUEST);
		}

	}

	@GET
	@Path("/all")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response get(@QueryParam(ZuliaConstants.INDEX) final String indexName, @QueryParam(ZuliaConstants.QUERY) String query) {

		StreamingOutput stream = output -> {

			Document filter;
			if (query != null) {
				filter = Document.parse(query);
			}
			else {
				filter = new Document();
			}

			indexManager.getAssociatedDocuments(indexName, output, filter);
		};

		return Response.ok(stream).build();

	}
}

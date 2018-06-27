package io.zulia.server.rest;

import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.io.Streamable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.zulia.ZuliaConstants;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.util.StreamHelper;
import org.bson.Document;

import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Controller(ZuliaConstants.ASSOCIATED_DOCUMENTS_URL)
public class AssociatedResource {

	private final static Logger LOG = Logger.getLogger(AssociatedResource.class.getSimpleName());

	@Singleton
	private ZuliaIndexManager indexManager;

	public AssociatedResource(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@Get
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public HttpResponse get(@Parameter(ZuliaConstants.ID) final String uniqueId, @Parameter(ZuliaConstants.FILE_NAME) final String fileName,
			@Parameter(ZuliaConstants.INDEX) final String indexName) {

		Streamable stream = ((outputStream, charset) -> {
			if (uniqueId != null && fileName != null && indexName != null) {
				InputStream is = indexManager.getAssociatedDocumentStream(indexName, uniqueId, fileName);
				if (is != null) {
					StreamHelper.copyStream(is, outputStream);
				}
				else {
					throw new IOException("Cannot find associated document with uniqueId <" + uniqueId + "> with fileName <" + fileName + ">");
				}
			}
			else {
				throw new IOException(ZuliaConstants.ID + " and " + ZuliaConstants.FILE_NAME + " are required");
			}
		});

		return HttpResponse.created(stream).status(ZuliaConstants.SUCCESS).header("content-disposition", "attachment; filename = " + fileName)
				.contentType(MediaType.APPLICATION_OCTET_STREAM);
	}

	@Post
	@Produces(MediaType.TEXT_XML)
	public HttpResponse post(@Parameter(ZuliaConstants.ID) String uniqueId, @Parameter(ZuliaConstants.FILE_NAME) String fileName,
			@Parameter(ZuliaConstants.INDEX) String indexName, @Parameter(ZuliaConstants.META) List<String> meta, @Body InputStream is) throws Exception {
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
						throw new Exception("Meta must be in the form key:value");
					}
				}
			}

			try {

				indexManager.storeAssociatedDocument(indexName, uniqueId, fileName, is, metaMap);

				return HttpResponse.created("Stored associated document with uniqueId <" + uniqueId + "> and fileName <" + fileName + ">")
						.status(ZuliaConstants.SUCCESS);

			}
			catch (Exception e) {
				LOG.log(Level.SEVERE, e.getMessage(), e);
				return HttpResponse.created(e.getMessage()).status(ZuliaConstants.INTERNAL_ERROR);
			}
		}
		else {
			throw new Exception(ZuliaConstants.ID + " and " + ZuliaConstants.FILE_NAME + " are required");
		}

	}

	@Get("/all")
	@Produces(MediaType.APPLICATION_JSON)
	public HttpResponse get(@Parameter(ZuliaConstants.INDEX) final String indexName, @Parameter(ZuliaConstants.QUERY) String query) {

		Streamable stream = (outputStream, charset) -> {
			Document filter;
			if (query != null) {
				filter = Document.parse(query);
			}
			else {
				filter = new Document();
			}

			indexManager.getAssociatedDocuments(indexName, outputStream, filter);
		};

		return HttpResponse.created(stream).status(ZuliaConstants.SUCCESS);

	}
}

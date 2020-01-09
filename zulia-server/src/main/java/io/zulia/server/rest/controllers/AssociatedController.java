package io.zulia.server.rest.controllers;

import io.micronaut.core.io.Streamable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.zulia.ZuliaConstants;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import io.zulia.util.StreamHelper;
import org.bson.Document;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Controller(ZuliaConstants.ASSOCIATED_DOCUMENTS_URL)
public class AssociatedController {

	private final static Logger LOG = Logger.getLogger(AssociatedController.class.getSimpleName());

	@Get
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public HttpResponse<?> get(@QueryValue(ZuliaConstants.ID) final String uniqueId, @QueryValue(ZuliaConstants.FILE_NAME) final String fileName,
			@QueryValue(ZuliaConstants.INDEX) final String indexName) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		try {
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

			return HttpResponse.ok(stream).status(ZuliaConstants.SUCCESS).header("content-disposition", "attachment; filename = " + fileName)
					.contentType(MediaType.APPLICATION_OCTET_STREAM);
		}
		catch (Exception e) {
			return HttpResponse.serverError(e.getMessage());
		}
	}

	@Post
	@Produces(MediaType.TEXT_XML)
	public HttpResponse<?> post(@QueryValue(ZuliaConstants.ID) String uniqueId, @QueryValue(ZuliaConstants.FILE_NAME) String fileName,
			@QueryValue(ZuliaConstants.INDEX) String indexName, @Nullable @QueryValue(ZuliaConstants.META_JSON) String metaJson, @Body InputStream is)
			throws Exception {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		if (uniqueId != null && fileName != null && indexName != null) {

			Document metadata;
			if (metaJson != null) {
				metadata = Document.parse(metaJson);
			}
			else {
				metadata = new Document();
			}

			try {

				indexManager.storeAssociatedDocument(indexName, uniqueId, fileName, is, metadata);

				return HttpResponse.ok("Stored associated document with uniqueId <" + uniqueId + "> and fileName <" + fileName + ">")
						.status(ZuliaConstants.SUCCESS);

			}
			catch (Exception e) {
				LOG.log(Level.SEVERE, e.getMessage(), e);
				return HttpResponse.ok(e.getMessage()).status(ZuliaConstants.INTERNAL_ERROR);
			}
		}
		else {
			throw new Exception(ZuliaConstants.ID + " and " + ZuliaConstants.FILE_NAME + " are required");
		}

	}

	@Get("/all")
	@Produces(MediaType.APPLICATION_JSON)
	public HttpResponse<?> get(@QueryValue(ZuliaConstants.INDEX) final String indexName, @Nullable @QueryValue(ZuliaConstants.QUERY) String query) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

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

		return HttpResponse.ok(stream).status(ZuliaConstants.SUCCESS);

	}
}

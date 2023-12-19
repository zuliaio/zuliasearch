package io.zulia.server.rest.controllers;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.io.Writable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.multipart.StreamingFileUpload;
import io.micronaut.http.server.types.files.StreamedFile;
import io.zulia.ZuliaRESTConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.server.exceptions.IndexDoesNotExistException;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
@Controller(ZuliaRESTConstants.ASSOCIATED_DOCUMENTS_URL)
public class AssociatedController {

	private final static Logger LOG = LoggerFactory.getLogger(AssociatedController.class);

	@Get("/metadata")
	@Produces(MediaType.APPLICATION_JSON)
	public HttpResponse<?> getAssociatedMetadata(@QueryValue(ZuliaRESTConstants.ID) final String uniqueId,
			@QueryValue(ZuliaRESTConstants.FILE_NAME) final String fileName, @QueryValue(ZuliaRESTConstants.INDEX) final String indexName) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		try {

			if (uniqueId != null && fileName != null && indexName != null) {
				ZuliaBase.AssociatedDocument associatedDocument = indexManager.getAssociatedDocument(indexName, uniqueId, fileName);

				if (associatedDocument != null) {
					byte[] metadataBSONBytes = associatedDocument.getMetadata().toByteArray();
					Document metadataDocument = ZuliaUtil.byteArrayToMongoDocument(metadataBSONBytes);
					String metadataJson = ZuliaUtil.mongoDocumentToJson(metadataDocument);
					return HttpResponse.ok(metadataJson);
				}
				else {
					return HttpResponse.notFound();
				}

			}
			else {
				return HttpResponse.badRequest(ZuliaRESTConstants.ID + " and " + ZuliaRESTConstants.FILE_NAME + " are required");
			}
		}
		catch (Exception e) {
			return HttpResponse.serverError(e.getMessage());
		}

	}

	@Get
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public HttpResponse<?> getAssociatedFile(@QueryValue(ZuliaRESTConstants.ID) final String uniqueId,
			@QueryValue(ZuliaRESTConstants.FILE_NAME) final String fileName, @QueryValue(ZuliaRESTConstants.INDEX) final String indexName) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		if (uniqueId != null && fileName != null && indexName != null) {
			try {
				InputStream is = indexManager.getAssociatedDocumentStream(indexName, uniqueId, fileName);
				if (is != null) {
					StreamedFile attach = new StreamedFile(is, MediaType.of(MediaType.ALL_TYPE)).attach(fileName);
					MutableHttpResponse<StreamedFile> ok = HttpResponse.ok(attach);
					attach.process(ok);
					return ok;
				}
				return HttpResponse.notFound();
			}
			catch (IndexDoesNotExistException e) {
				return HttpResponse.notFound(e.getMessage());
			}
			catch (Exception e) {
				return HttpResponse.serverError(e.getMessage());
			}
		}
		else {
			return HttpResponse.badRequest(ZuliaRESTConstants.ID + " and " + ZuliaRESTConstants.FILE_NAME + " are required");
		}

	}

	@Get("/allForId")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public HttpResponse<?> getAssociatedForId(@QueryValue(ZuliaRESTConstants.ID) final String uniqueId,
			@QueryValue(ZuliaRESTConstants.INDEX) final String indexName) {
		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();
		try {
			if (uniqueId != null && indexName != null) {
				List<String> associatedDocuments = indexManager.getAssociatedFilenames(indexName, uniqueId);
				JsonObject jsonObject = new JsonObject();
				JsonArray jsonArray = new JsonArray();
				for (String filename : associatedDocuments) {
					jsonArray.add(filename);
				}
				jsonObject.add("filenames", jsonArray);
				return HttpResponse.ok(jsonObject);
			}
			else {
				return HttpResponse.badRequest(ZuliaRESTConstants.ID + " and " + ZuliaRESTConstants.INDEX + " are required");
			}
		}
		catch (IndexDoesNotExistException e) {
			return HttpResponse.badRequest(e.getMessage());
		}
		catch (Exception e) {
			return HttpResponse.serverError(e.getMessage());
		}
	}

	@Post(consumes = MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.TEXT_PLAIN)
	public Publisher<HttpResponse<?>> storeAssociated(StreamingFileUpload file, String id, String fileName, String indexName, String metaJson) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		if (id != null && fileName != null && indexName != null) {

			Document metaDoc;
			if (metaJson != null) {
				metaDoc = Document.parse(metaJson);
			}
			else {
				metaDoc = new Document();
			}

			OutputStream associatedDocumentOutputStream;
			try {
				associatedDocumentOutputStream = indexManager.getAssociatedDocumentOutputStream(indexName, id, fileName, metaDoc);

				Publisher<Boolean> uploadPublisher = file.transferTo(associatedDocumentOutputStream);
				return Flux.from(uploadPublisher).publishOn(Schedulers.boundedElastic()).map(success -> {
					if (success) {
						try {
							associatedDocumentOutputStream.close();
						}
						catch (IOException e) {
							LOG.error("Failed to close stream: " + e.getMessage(), e);
						}
						return HttpResponse.ok("Stored associated document with uniqueId <" + id + "> and fileName <" + fileName + ">")
								.status(ZuliaRESTConstants.SUCCESS);

					}
					else {
						try {
							associatedDocumentOutputStream.close();
						}
						catch (IOException e) {
							LOG.error("Failed to close stream: " + e.getMessage(), e);
						}
						return HttpResponse.serverError("Failed to store associated document with uniqueId <" + id + "> and filename <" + fileName + ">");
					}

				});

			}
			catch (Exception e) {
				LOG.error(e.getMessage(), e);
				return Mono.just(HttpResponse.serverError("Failed to store <" + id + "> in index <" + indexName + "> for file <" + fileName + ">"));
			}

		}
		else {
			return Mono.just(HttpResponse.badRequest(ZuliaRESTConstants.ID + " and " + ZuliaRESTConstants.FILE_NAME + " are required"));
		}

	}

	@Get("/all")
	@Produces(MediaType.APPLICATION_JSON)
	public HttpResponse<?> getAllAssociatedForIndex(@QueryValue(ZuliaRESTConstants.INDEX) final String indexName,
			@Nullable @QueryValue(ZuliaRESTConstants.QUERY) String query) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		Writable writable = out -> {
			Document filter;
			if (query != null) {
				filter = Document.parse(query);
			}
			else {
				filter = new Document();
			}
			try {
				indexManager.getAssociatedFilenames(indexName, out, filter);
			}
			catch (Exception e) {
				LOG.error(e.getMessage(), e);
				HttpResponse.serverError(e.getMessage());
			}
		};

		return HttpResponse.ok(writable).status(ZuliaRESTConstants.SUCCESS);

	}

}

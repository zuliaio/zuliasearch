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
import io.micronaut.http.multipart.MultipartException;
import io.micronaut.http.multipart.PartData;
import io.micronaut.http.multipart.StreamingFileUpload;
import io.micronaut.http.server.types.files.StreamedFile;
import io.zulia.ZuliaConstants;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import org.bson.Document;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.util.List;
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

			if (uniqueId != null && fileName != null && indexName != null) {
				InputStream is = indexManager.getAssociatedDocumentStream(indexName, uniqueId, fileName);
				StreamedFile attach = new StreamedFile(is, MediaType.of(MediaType.ALL_TYPE)).attach(fileName);
				MutableHttpResponse<StreamedFile> ok = HttpResponse.ok(attach);
				attach.process(ok);
				return ok;
			}
			else {
				return HttpResponse.serverError(ZuliaConstants.ID + " and " + ZuliaConstants.FILE_NAME + " are required");
			}
		}
		catch (Exception e) {
			return HttpResponse.serverError(e.getMessage());
		}
	}

	@Get("/allForId")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public HttpResponse<?> get(@QueryValue(ZuliaConstants.ID) final String uniqueId, @QueryValue(ZuliaConstants.INDEX) final String indexName) {
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
				return HttpResponse.serverError("Provide uniqueId and index.");
			}
		}
		catch (Exception e) {
			return HttpResponse.serverError(e.getMessage());
		}
	}

	@Post(consumes = MediaType.MULTIPART_FORM_DATA)
	public Publisher<Boolean> post(@QueryValue(ZuliaConstants.ID) String id, @QueryValue(ZuliaConstants.FILE_NAME) String fileName,
			@QueryValue(ZuliaConstants.INDEX) String indexName, @Nullable @QueryValue(ZuliaConstants.META_JSON) String metaJson, StreamingFileUpload file)
			throws Exception {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		if (id != null && fileName != null && indexName != null) {

			Document metadata;
			if (metaJson != null) {
				metadata = Document.parse(metaJson);
			}
			else {
				metadata = new Document();
			}

			return Mono.<Boolean>create(emitter ->

					file.subscribe(new Subscriber<>() {
						Subscription subscription;

						@Override
						public void onSubscribe(Subscription s) {
							subscription = s;
							subscription.request(1);
						}

						@Override
						public void onNext(PartData o) {
							try {
								indexManager.storeAssociatedDocument(indexName, id, fileName, o.getInputStream(), metadata);
								subscription.request(1);
							}
							catch (Exception e) {
								handleError(e);
							}
						}

						@Override
						public void onError(Throwable t) {
							emitter.error(t);
							try {

							}
							catch (Exception e) {
								e.printStackTrace();
							}
						}

						@Override
						public void onComplete() {
							try {
								System.out.println("Finished upload");
								emitter.success(true);
							}
							catch (Exception e) {
								emitter.success(false);
							}
						}

						private void handleError(Throwable t) {
							subscription.cancel();
							onError(new MultipartException("Error transferring file: " + file.getName(), t));
						}
					})).flux();

			/*
			File tempFile = File.createTempFile(file.getFilename(), "upload_temp");
			try {
				Publisher<Boolean> uploadPublisher = file.transferTo(tempFile);
				return Flux.from(uploadPublisher).map(success -> {
					if (success) {
						try (FileInputStream is = new FileInputStream(tempFile)) {
							indexManager.storeAssociatedDocument(indexName, id, fileName, is, metadata);
						}
						catch (Throwable t) {
							return HttpResponse.serverError(
									"Failed to store associated document with uniqueId <" + id + "> and filename <" + fileName + "> due to: " + t.getMessage());
						}
						tempFile.delete();
						return HttpResponse.ok("Stored associated document with uniqueId <" + id + "> and fileName <" + fileName + ">")
								.status(ZuliaConstants.SUCCESS);
					}
					else {
						tempFile.delete();
						return HttpResponse.serverError("Failed to store associated document with uniqueId <" + id + "> and filename <" + fileName + ">");
					}

				});

			}
			catch (Exception e) {
				tempFile.delete();
				LOG.log(Level.SEVERE, e.getMessage(), e);
				throw e;
			}
			 */

		}
		else {
			throw new Exception(ZuliaConstants.ID + " and " + ZuliaConstants.FILE_NAME + " are required");
		}

	}

	@Get("/all")
	@Produces(MediaType.APPLICATION_JSON)
	public HttpResponse<?> getAll(@QueryValue(ZuliaConstants.INDEX) final String indexName, @Nullable @QueryValue(ZuliaConstants.QUERY) String query) {

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
				LOG.log(Level.SEVERE, e.getMessage(), e);
				HttpResponse.serverError(e.getMessage());
			}
		};

		return HttpResponse.ok(writable).status(ZuliaConstants.SUCCESS);

	}

}

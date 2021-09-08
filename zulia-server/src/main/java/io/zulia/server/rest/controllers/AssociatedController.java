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
import io.micronaut.scheduling.TaskExecutors;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.bson.Document;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Controller(ZuliaConstants.ASSOCIATED_DOCUMENTS_URL)
public class AssociatedController {

	private final static Logger LOG = Logger.getLogger(AssociatedController.class.getSimpleName());

	@Inject
	@Named(TaskExecutors.IO)
	ExecutorService ioExecutor;

	//work around for
	//https://github.com/micronaut-projects/micronaut-core/issues/6084
	public static Publisher<Boolean> transferToStream(ExecutorService ioExecutor, StreamingFileUpload fileUpload, OutputStream outputStream) {

		return Mono.<Boolean>create(emitter ->

				Flux.from(fileUpload).subscribeOn(Schedulers.fromExecutorService(ioExecutor)).subscribe(new Subscriber<>() {
					Subscription subscription;

					@Override
					public void onSubscribe(Subscription s) {
						subscription = s;
						subscription.request(1);
					}

					@Override
					public void onNext(PartData o) {
						try {
							outputStream.write(o.getBytes());
							subscription.request(1);
						}
						catch (IOException e) {
							handleError(e);
						}
					}

					@Override
					public void onError(Throwable t) {
						emitter.error(t);
						try {
							if (outputStream != null) {
								outputStream.close();
							}
						}
						catch (IOException e) {
							System.err.println("Failed to close file stream : " + fileUpload.getName());
						}
					}

					@Override
					public void onComplete() {
						try {
							outputStream.close();
							emitter.success(true);
						}
						catch (IOException e) {
							System.err.println("Failed to close file stream : " + fileUpload.getName());
							emitter.success(false);
						}
					}

					private void handleError(Throwable t) {
						subscription.cancel();
						onError(new MultipartException("Error transferring file: " + fileUpload.getName(), t));
					}
				})).flux();

	}

	@Get("/metadata")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public HttpResponse<?> getMetadata(@QueryValue(ZuliaConstants.ID) final String uniqueId, @QueryValue(ZuliaConstants.FILE_NAME) final String fileName,
			@QueryValue(ZuliaConstants.INDEX) final String indexName) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		try {

			if (uniqueId != null && fileName != null && indexName != null) {
				ZuliaBase.AssociatedDocument associatedDocument = indexManager.getAssociatedDocument(indexName, uniqueId, fileName);
				StreamedFile attach = new StreamedFile(new ByteArrayInputStream(associatedDocument.getMetadata().toByteArray()),
						MediaType.of(MediaType.APPLICATION_JSON)).attach(fileName);
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
	@Produces(MediaType.TEXT_PLAIN)
	public Publisher<HttpResponse<?>> post(StreamingFileUpload file, Map<String, Object> metadata) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		String id = metadata.get("id").toString();
		String fileName = metadata.get("fileName").toString();
		String indexName = metadata.get("indexName").toString();

		if (id != null && fileName != null && indexName != null) {

			Document metaDoc;
			if (metadata.containsKey("metaJson")) {
				metaDoc = Document.parse(metadata.get("metaJson").toString());
			}
			else {
				metaDoc = new Document();
			}

			OutputStream associatedDocumentOutputStream;
			try {
				associatedDocumentOutputStream = indexManager.getAssociatedDocumentOutputStream(indexName, id, fileName, metaDoc);
				Publisher<Boolean> uploadPublisher = transferToStream(ioExecutor, file, associatedDocumentOutputStream);
				return Flux.from(uploadPublisher).map(success -> {
					if (success) {
						try {
							associatedDocumentOutputStream.close();
						}
						catch (IOException e) {
							LOG.log(Level.SEVERE, "Failed to close stream: " + e.getMessage(), e);
						}
						return HttpResponse.ok("Stored associated document with uniqueId <" + id + "> and fileName <" + fileName + ">")
								.status(ZuliaConstants.SUCCESS);

					}
					else {
						try {
							associatedDocumentOutputStream.close();
						}
						catch (IOException e) {
							LOG.log(Level.SEVERE, "Failed to close stream: " + e.getMessage(), e);
						}
						return HttpResponse.serverError("Failed to store associated document with uniqueId <" + id + "> and filename <" + fileName + ">");
					}

				});

			}
			catch (Exception e) {
				LOG.log(Level.SEVERE, e.getMessage(), e);
				return Mono.just(HttpResponse.serverError("Failed to store <" + id + "> in index <" + indexName + "> for file <" + fileName + ">"));
			}

		}
		else {
			return Mono.just(HttpResponse.serverError(ZuliaConstants.ID + " and " + ZuliaConstants.FILE_NAME + " are required"));
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

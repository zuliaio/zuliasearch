package io.zulia.server.rest.controllers;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.multipart.StreamingFileUpload;
import io.micronaut.http.server.types.files.StreamedFile;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.zulia.ZuliaRESTConstants;
import io.zulia.rest.dto.AssociatedMetadataDTO;
import io.zulia.server.exceptions.AssociatedDocumentDoesNotExistException;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
@Controller(ZuliaRESTConstants.ASSOCIATED_URL)
@Tag(name = "Associated")
@ApiResponses({ @ApiResponse(responseCode = "400", content = { @Content(schema = @Schema(implementation = io.micronaut.http.hateoas.JsonError.class)) }),
		@ApiResponse(responseCode = "404", content = { @Content(schema = @Schema(implementation = io.micronaut.http.hateoas.JsonError.class)) }),
		@ApiResponse(responseCode = "500", content = { @Content(schema = @Schema(implementation = io.micronaut.http.hateoas.JsonError.class)) }),
		@ApiResponse(responseCode = "503", content = { @Content(schema = @Schema(implementation = io.micronaut.http.hateoas.JsonError.class)) }) })
public class AssociatedController {

	private final static Logger LOG = LoggerFactory.getLogger(AssociatedController.class);

	@Get("/{indexName}/{uniqueId}/{fileName}/metadata")
	@ExecuteOn(TaskExecutors.VIRTUAL)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Get associated document metadata", description = "Returns the metadata JSON for a specific associated file")
	public String getAssociatedMetadata(String indexName, String uniqueId, String fileName) throws Exception {
		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();
		Document metadataDocument = indexManager.getAssociatedDocumentMeta(indexName, uniqueId, fileName);
		if (metadataDocument == null) {
			throw new AssociatedDocumentDoesNotExistException(indexName, uniqueId, fileName);

		}
		return ZuliaUtil.mongoDocumentToJson(metadataDocument);
	}

	@Get("/{indexName}/{uniqueId}/{fileName}/file")
	@ExecuteOn(TaskExecutors.VIRTUAL)
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	@Operation(summary = "Download associated file", description = "Downloads the associated file content as a binary stream")
	public StreamedFile getAssociatedFile(String indexName, String uniqueId, String fileName) throws Exception {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		InputStream is = indexManager.getAssociatedDocumentStream(indexName, uniqueId, fileName);
		if (is == null) {
			throw new AssociatedDocumentDoesNotExistException(indexName, uniqueId, fileName);

		}
		return new StreamedFile(is, MediaType.of(MediaType.ALL_TYPE)).attach(fileName);
	}

	public record Filenames(List<String> filenames) {

	}

	@Get("/{indexName}/{uniqueId}/filenames")
	@ExecuteOn(TaskExecutors.VIRTUAL)
	@Produces(MediaType.TEXT_JSON)
	@Operation(summary = "List associated filenames", description = "Returns the list of associated filenames for a given document")
	public Filenames getAssociatedFileNamesForId(final String uniqueId, String indexName) throws Exception {
		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		List<String> associatedDocuments = indexManager.getAssociatedFilenames(indexName, uniqueId);
		return new Filenames(associatedDocuments);
	}

	@Get("/{indexName}/{uniqueId}/bundle")
	@ExecuteOn(TaskExecutors.VIRTUAL)
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	@Operation(summary = "Download associated files bundle", description = "Downloads all associated files for a document as a ZIP archive, including metadata")
	public StreamedFile getAssociatedBundleForId(final String uniqueId, String indexName) throws Exception {
		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		PipedOutputStream output = new PipedOutputStream();
		PipedInputStream input = new PipedInputStream(output);

		Thread.startVirtualThread(() -> {
			try (output; ZipOutputStream zipOutputStream = new ZipOutputStream(output)) {
				List<String> fileNames = indexManager.getAssociatedFilenames(indexName, uniqueId);
				for (String fileName : fileNames) {
					String fileDir = fileName + File.separator;
					zipOutputStream.putNextEntry(new ZipEntry(fileDir));
					zipOutputStream.putNextEntry(new ZipEntry(fileDir + fileName));

					try (InputStream is = indexManager.getAssociatedDocumentStream(indexName, uniqueId, fileName)) {
						is.transferTo(zipOutputStream);
					}

					Document metadataDocument = indexManager.getAssociatedDocumentMeta(indexName, uniqueId, fileName);
					if (metadataDocument != null && !metadataDocument.isEmpty()) {
						zipOutputStream.putNextEntry(new ZipEntry(fileDir + fileName + "_metadata.json"));
						zipOutputStream.write(metadataDocument.toJson().getBytes(StandardCharsets.UTF_8));
					}

				}
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		return new StreamedFile(input, MediaType.APPLICATION_OCTET_STREAM_TYPE);
	}

	@Get("/{indexName}/all")
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "List all associated documents", description = "Returns metadata for all associated documents in the index, optionally filtered by a query")
	public Flux<AssociatedMetadataDTO> getAllAssociatedForIndex(String indexName, @Nullable @QueryValue(ZuliaRESTConstants.QUERY) String query)
			throws Exception {
		Document queryDoc = (query != null) ? Document.parse(query) : new Document();
		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();
		return Flux.fromStream(indexManager.getAssociatedFilenames(indexName, queryDoc));
	}

	@Post("/{indexName}/{uniqueId}/{fileName}")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.TEXT_PLAIN)
	@Operation(summary = "Store an associated file", description = "Uploads a file and associates it with a document, optionally including metadata JSON")
	public Publisher<HttpResponse<?>> storeAssociated(StreamingFileUpload file, String uniqueId, String fileName, String indexName, @Nullable String metaJson)
			throws Exception {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();
		Document metaDoc = (metaJson != null) ? Document.parse(metaJson) : new Document();
		OutputStream associatedDocumentOutputStream = indexManager.getAssociatedDocumentOutputStream(indexName, uniqueId, fileName, metaDoc);
		Publisher<Boolean> uploadPublisher = file.transferTo(associatedDocumentOutputStream);

		return Mono.from(uploadPublisher).map(success -> {
			if (success) {
				return HttpResponse.ok();
			}
			else {
				return HttpResponse.serverError();
			}
		});

	}

}

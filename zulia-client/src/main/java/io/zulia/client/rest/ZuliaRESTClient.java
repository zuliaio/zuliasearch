package io.zulia.client.rest;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.multipart.MultipartBody;
import io.zulia.ZuliaConstants;
import io.zulia.util.HttpHelper;
import org.bson.Document;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static io.micronaut.http.HttpRequest.GET;

public class ZuliaRESTClient {

	private static final Logger LOG = Logger.getLogger(ZuliaRESTClient.class.getName());

	private final MicronautHttpClient client;
	private final String url;

	public ZuliaRESTClient(String server, int restPort) {
		url = "http://" + server + ":" + restPort;

		client = MicronautHttpClient.createClient(url);

	}

	public void fetchAssociated(String uniqueId, String indexName, String fileName, OutputStream destination, boolean closeStream) throws Exception {

		try {

			Flux<HttpResponse<byte[]>> data = Flux.from(client.exchange(
					GET(ZuliaConstants.ASSOCIATED_DOCUMENTS_URL + "?" + HttpHelper.createQuery(createParameters(uniqueId, indexName, fileName))),
					byte[].class));

			data.subscribe(httpResponse -> {
				try {
					destination.write(Objects.requireNonNull(httpResponse.body(), "No body for file"));
				}
				catch (IOException e) {
					throw new RuntimeException("Failed to fetch <" + fileName + "> for id <" + uniqueId + "> for index <" + indexName + ">: " + e.getMessage());
				}
			}, throwable -> {
				throw new RuntimeException(
						"Failed to fetch <" + fileName + "> for id <" + uniqueId + "> for index <" + indexName + ">: " + throwable.getMessage());
			});
		}
		finally {
			if (closeStream) {
				destination.close();
			}
		}
	}

	public void fetchAssociated(String uniqueId, String indexName, OutputStream destination, boolean closeStream) throws Exception {

		String allIdsJson = client.toBlocking()
				.retrieve(GET(ZuliaConstants.ASSOCIATED_DOCUMENTS_ALL_FOR_ID_URL + "?" + HttpHelper.createQuery(createParameters(uniqueId, indexName))));
		JsonObject result = JsonParser.parseString(allIdsJson).getAsJsonObject();
		JsonArray filenames = result.getAsJsonArray("filenames");

		ZipOutputStream zipOutputStream = null;
		try {
			zipOutputStream = new ZipOutputStream(destination);

			for (int i = 0; i < filenames.size(); i++) {
				String filename = filenames.get(i).getAsString();
				zipOutputStream.putNextEntry(new ZipEntry(filename));
				fetchAssociated(uniqueId, indexName, filename, zipOutputStream, false);
			}

		}
		finally {
			if (closeStream) {
				if (zipOutputStream != null) {
					zipOutputStream.close();
				}
			}
		}

	}

	public void storeAssociated(String uniqueId, String indexName, String fileName, Document metadata, InputStream source) throws Exception {

		MultipartBody.Builder builder = MultipartBody.builder().addPart("id", uniqueId).addPart("index", indexName).addPart("fileName", fileName)
				.addPart("file", fileName, MediaType.forFilename(fileName), source, 0);

		if (metadata != null) {
			builder.addPart("metaJson", metadata.toJson());
		}

		try (source) {
			client.toBlocking().exchange(HttpRequest.POST(ZuliaConstants.ASSOCIATED_DOCUMENTS_URL, builder.build()).contentType(MediaType.MULTIPART_FORM_DATA),
					String.class);
		}
		catch (Exception e) {
			System.err.println("Failed to store file <" + fileName + ">");
			LOG.log(Level.SEVERE, "Failed to store file <" + fileName + ">", e);
			throw e;
		}

	}

	private HashMap<String, Object> createParameters(String uniqueId, String indexName) {
		HashMap<String, Object> parameters = new HashMap<>();
		parameters.put(ZuliaConstants.ID, uniqueId);
		parameters.put(ZuliaConstants.INDEX, indexName);
		return parameters;
	}

	private HashMap<String, Object> createParameters(String uniqueId, String indexName, String fileName) {
		HashMap<String, Object> parameters = new HashMap<>();
		parameters.put(ZuliaConstants.ID, uniqueId);
		parameters.put(ZuliaConstants.FILE_NAME, fileName);
		parameters.put(ZuliaConstants.INDEX, indexName);
		return parameters;
	}

	public void close() {
		LOG.info("Closing REST client pool to " + url);
		client.close();
	}

}

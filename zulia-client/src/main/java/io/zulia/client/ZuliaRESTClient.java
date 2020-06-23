package io.zulia.client;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.DefaultHttpClientConfiguration;
import io.micronaut.http.client.HttpClientConfiguration;
import io.micronaut.http.client.multipart.MultipartBody;
import io.micronaut.http.client.netty.DefaultHttpClient;
import io.micronaut.runtime.ApplicationConfiguration;
import io.reactivex.Flowable;
import io.zulia.ZuliaConstants;
import io.zulia.util.HttpHelper;
import org.bson.Document;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.Objects;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static io.micronaut.http.HttpRequest.GET;
import static java.time.temporal.ChronoUnit.SECONDS;

public class ZuliaRESTClient {

	private static final Logger LOG = Logger.getLogger(ZuliaRESTClient.class.getName());

	private final DefaultHttpClient client;

	public ZuliaRESTClient(String server, int restPort) {
		URL uri;
		try {
			String urlString = "http://" + server + ":" + restPort;
			LOG.info("Opening REST pool to " + urlString);
			uri = new URI(urlString).toURL();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		DefaultHttpClientConfiguration.DefaultConnectionPoolConfiguration defaultConnectionPoolConfiguration = new DefaultHttpClientConfiguration.DefaultConnectionPoolConfiguration();
		defaultConnectionPoolConfiguration.setEnabled(true);
		defaultConnectionPoolConfiguration.setAcquireTimeout(Duration.of(60, SECONDS));
		defaultConnectionPoolConfiguration.setMaxConnections(64);
		defaultConnectionPoolConfiguration.setMaxPendingAcquires(64);
		ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration();
		HttpClientConfiguration clientConfiguration = new DefaultHttpClientConfiguration(defaultConnectionPoolConfiguration, applicationConfiguration);
		clientConfiguration.setMaxContentLength(1024 * 1024 * 1024);
		clientConfiguration.setReadTimeout(Duration.ofSeconds(300));

		client = new DefaultHttpClient(uri, clientConfiguration);
	}

	public void fetchAssociated(String uniqueId, String indexName, String fileName, OutputStream destination, boolean closeStream) throws Exception {

		try {
			Flowable<HttpResponse<byte[]>> data = client
					.exchange(GET(ZuliaConstants.ASSOCIATED_DOCUMENTS_URL + "?" + HttpHelper.createQuery(createParameters(uniqueId, indexName, fileName))),
							byte[].class);

			data.blockingSubscribe(httpResponse -> {
				destination.write(Objects.requireNonNull(httpResponse.body(), "No body for file"));

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

		try (source) {

			MultipartBody.Builder builder = MultipartBody.builder().addPart("id", uniqueId).addPart("index", indexName).addPart("fileName", fileName)
					.addPart("file", fileName, MediaType.forFilename(fileName), source, 0);

			if (metadata != null) {
				builder.addPart("metaJson", metadata.toJson());
			}

			client.toBlocking().exchange(HttpRequest.POST(ZuliaConstants.ASSOCIATED_DOCUMENTS_URL, builder.build()).contentType(MediaType.MULTIPART_FORM_DATA),
					String.class);

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
		client.close();
	}

}

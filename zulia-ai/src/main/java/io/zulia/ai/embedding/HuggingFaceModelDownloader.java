package io.zulia.ai.embedding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

public class HuggingFaceModelDownloader {

	private static final Logger LOG = LoggerFactory.getLogger(HuggingFaceModelDownloader.class);

	private static final String HF_BASE = "https://huggingface.co/";
	private static final String[] ONNX_PATHS = { "onnx/model.onnx", "model.onnx" };

	private static final Path CACHE_DIR = resolveCacheDir();

	private static Path resolveCacheDir() {
		String djlCache = System.getenv("DJL_CACHE_DIR");
		if (djlCache == null) {
			djlCache = System.getProperty("DJL_CACHE_DIR");
		}
		if (djlCache != null) {
			return Path.of(djlCache, "huggingface");
		}
		return Path.of(System.getProperty("user.home"), ".djl.ai", "huggingface");
	}

	public static Path downloadModel(String modelUrl) throws IOException {
		String modelId = extractModelId(modelUrl);
		Path modelDir = CACHE_DIR.resolve(modelId.replace("/", "_"));
		Path onnxFile = modelDir.resolve("model.onnx");
		Path tokenizerFile = modelDir.resolve("tokenizer.json");
		Path configFile = modelDir.resolve("config.json");

		Files.createDirectories(modelDir);

		if (!Files.exists(onnxFile)) {
			downloadOnnxModel(modelId, onnxFile);
			downloadExternalData(modelId, modelDir);
		}

		if (!Files.exists(tokenizerFile)) {
			downloadFile(HF_BASE + modelId + "/resolve/main/tokenizer.json", tokenizerFile);
		}

		// config.json drives model type detection but not every repository publishes one, and a cache
		// directory filled before it was fetched must still pick it up, so it is tried on every load
		if (!Files.exists(configFile)) {
			try {
				downloadFile(HF_BASE + modelId + "/resolve/main/config.json", configFile);
			}
			catch (IOException e) {
				LOG.info("No config.json for {}, model type detection will use its default: {}", modelId, e.getMessage());
			}
		}

		return modelDir;
	}

	private static void downloadOnnxModel(String modelId, Path target) throws IOException {
		IOException failure = new IOException("No ONNX model found for " + modelId + " (tried: " + String.join(", ", ONNX_PATHS) + ")");
		for (String onnxPath : ONNX_PATHS) {
			String url = HF_BASE + modelId + "/resolve/main/" + onnxPath;
			try {
				downloadFile(url, target);
				return;
			}
			catch (IOException e) {
				// the summary names every path and carries each real cause
				failure.addSuppressed(e);
			}
		}
		throw failure;
	}

	/**
	 * Large exports keep their weights in an external data file next to the graph, which the graph references by
	 * name, so it must sit in the same directory as model.onnx. Fetched when the repository has one.
	 */
	private static void downloadExternalData(String modelId, Path modelDir) throws IOException {
		Path dataFile = modelDir.resolve("model.onnx_data");
		if (Files.exists(dataFile)) {
			return;
		}
		for (String onnxPath : ONNX_PATHS) {
			try {
				downloadFile(HF_BASE + modelId + "/resolve/main/" + onnxPath + "_data", dataFile);
				return;
			}
			catch (IOException e) {
				// no external data at this path, try the next layout or accept a single file model
			}
		}
	}

	private static void downloadFile(String url, Path target) throws IOException {
		// a per-download temp name keeps concurrent loads of the same model from truncating or renaming each other's partial body
		Path temp = target.resolveSibling(target.getFileName() + "." + UUID.randomUUID() + ".tmp");
		try (HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build()) {
			HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
			HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
			if (response.statusCode() != 200) {
				throw new IOException("HTTP " + response.statusCode() + " downloading " + url);
			}
			try (InputStream is = response.body()) {
				Files.copy(is, temp, StandardCopyOption.REPLACE_EXISTING);
			}
			Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException("Download interrupted: " + url, e);
		}
		finally {
			Files.deleteIfExists(temp);
		}
	}

	private static String extractModelId(String url) {
		if (url.startsWith(HF_BASE)) {
			String path = url.substring(HF_BASE.length());
			String[] parts = path.split("/");
			if (parts.length >= 2) {
				return parts[0] + "/" + parts[1];
			}
		}
		return url;
	}
}

package io.zulia.server.config.single;

import com.google.protobuf.util.JsonFormat;
import io.zulia.message.ZuliaIndex;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.ZuliaConfig;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FSIndexService implements IndexService {

	private static final Logger LOG = Logger.getLogger(FSIndexService.class.getName());
	private static final String EXTENSION = ".json";
	private static final String MAPPING_EXTENSION = "_mapping.json";
	private final String baseDir;

	public FSIndexService(ZuliaConfig zuliaConfig) {
		// create the base dir
		baseDir = zuliaConfig.getDataPath() + File.separator + SETTINGS;
		try {
			Files.createDirectory(Paths.get(baseDir));
		}
		catch (FileAlreadyExistsException e1) {
			// skip if the base dir exists.
		}
		catch (IOException e) {
			LOG.log(Level.SEVERE, e.getMessage(), e);
			throw new RuntimeException("Failed to create the base directory to store index settings.");
		}

	}

	@Override
	public List<ZuliaIndex.IndexSettings> getIndexes() throws IOException {

		if (Paths.get(baseDir).toFile().exists()) {
			JsonFormat.Parser parser = JsonFormat.parser();
			List<ZuliaIndex.IndexSettings> indexes = new ArrayList<>();
			for (File files : Paths.get(baseDir).toFile().listFiles()) {
				if (!files.getName().endsWith(MAPPING_EXTENSION)) {
					ZuliaIndex.IndexSettings.Builder indexSettings = ZuliaIndex.IndexSettings.newBuilder();
					parser.merge(new FileReader(files), indexSettings);
					indexes.add(indexSettings.build());
				}
			}
			return indexes;
		}

		return null;
	}

	@Override
	public void createIndex(ZuliaIndex.IndexSettings indexSettings) throws IOException {
		JsonFormat.Printer printer = JsonFormat.printer();
		String indexSettingsJson = printer.print(indexSettings);
		writeFile(indexSettingsJson, indexSettings.getIndexName() + EXTENSION);
	}

	@Override
	public void removeIndex(String indexName) throws IOException {
		Files.deleteIfExists(Paths.get(baseDir + File.separator + indexName + EXTENSION));
		Files.deleteIfExists(Paths.get(baseDir + File.separator + indexName + MAPPING_EXTENSION));
	}

	@Override
	public List<ZuliaIndex.IndexMapping> getIndexMappings() {

		if (Paths.get(baseDir).toFile().exists()) {
			JsonFormat.Parser parser = JsonFormat.parser();
			List<ZuliaIndex.IndexMapping> indexMappings = new ArrayList<>();
			for (File files : Paths.get(baseDir).toFile().listFiles()) {
				try {
					if (files.getName().endsWith(MAPPING_EXTENSION)) {
						ZuliaIndex.IndexMapping.Builder indexMapping = ZuliaIndex.IndexMapping.newBuilder();
						parser.merge(new FileReader(files), indexMapping);
						indexMappings.add(indexMapping.build());
					}
				}
				catch (Exception e) {
					LOG.log(Level.SEVERE, e.getMessage(), e);
				}
			}
			return indexMappings;
		}

		return null;

	}

	@Override
	public ZuliaIndex.IndexMapping getIndexMapping(String indexName) throws IOException {
		File indexMapping = new File(baseDir + File.separator + indexName + MAPPING_EXTENSION);
		if (indexMapping.exists()) {
			ZuliaIndex.IndexMapping.Builder indexMappingBuilder = ZuliaIndex.IndexMapping.newBuilder();
			JsonFormat.parser().merge(new FileReader(indexMapping), indexMappingBuilder);
			return indexMappingBuilder.build();
		}

		return null;
	}

	@Override
	public void storeIndexMapping(ZuliaIndex.IndexMapping indexMapping) throws IOException {
		JsonFormat.Printer printer = JsonFormat.printer();
		String indexMappingJson = printer.print(indexMapping);
		writeFile(indexMappingJson, indexMapping.getIndexName() + MAPPING_EXTENSION);
	}

	private void writeFile(String json, String filename) throws IOException {
		File file = new File(baseDir + File.separator + filename);
		try (FileWriter fileWriter = new FileWriter(file)) {
			fileWriter.write(json);
		}
	}
}

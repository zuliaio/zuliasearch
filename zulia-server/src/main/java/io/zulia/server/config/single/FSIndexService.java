package io.zulia.server.config.single;

import com.google.protobuf.util.JsonFormat;
import io.zulia.message.ZuliaIndex;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.ZuliaConfig;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FSIndexService implements IndexService {

	private static final Logger LOG = Logger.getLogger(FSIndexService.class.getName());
	private static final String SETTINGS = "settings";
	private static final String EXTENSION = ".json";
	private static final String MAPPING_EXTENSION = "_mapping.json";
	private final String baseDir;

	public FSIndexService(ZuliaConfig zuliaConfig) {
		// create the base dir
		baseDir = zuliaConfig.getDataPath() + File.separator + SETTINGS;
		new File(baseDir).mkdir();
	}

	@Override
	public List<ZuliaIndex.IndexSettings> getIndexes() {

		File settingsDir = new File(baseDir);

		if (settingsDir.exists()) {
			JsonFormat.Parser parser = JsonFormat.parser();
			List<ZuliaIndex.IndexSettings> indexes = new ArrayList<>();
			for (File files : settingsDir.listFiles()) {
				try {
					if (!files.getName().endsWith(MAPPING_EXTENSION)) {
						ZuliaIndex.IndexSettings.Builder indexSettings = ZuliaIndex.IndexSettings.newBuilder();
						parser.merge(new FileReader(files), indexSettings);
						indexes.add(indexSettings.build());
					}
				}
				catch (Exception e) {
					LOG.log(Level.SEVERE, e.getMessage(), e);
				}
			}
			return indexes;
		}

		return null;
	}

	@Override
	public void createIndex(ZuliaIndex.IndexSettings indexSettings) {
		try {
			JsonFormat.Printer printer = JsonFormat.printer();
			String indexSettingsJson = printer.print(indexSettings);
			writeFile(indexSettingsJson, indexSettings.getIndexName() + EXTENSION);
		}
		catch (Exception e) {
			LOG.log(Level.SEVERE, e.getMessage(), e);
		}

	}

	@Override
	public void removeIndex(String indexName) {
		new File(baseDir + File.separator + indexName + EXTENSION).delete();
		new File(baseDir + File.separator + indexName + MAPPING_EXTENSION).delete();
	}

	@Override
	public List<ZuliaIndex.IndexMapping> getIndexMappings() {

		File settingsDir = new File(baseDir);

		if (settingsDir.exists()) {
			JsonFormat.Parser parser = JsonFormat.parser();
			List<ZuliaIndex.IndexMapping> indexMappings = new ArrayList<>();
			for (File files : settingsDir.listFiles()) {
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
	public ZuliaIndex.IndexMapping getIndexMapping(String indexName) {
		File indexMapping = new File(baseDir + File.separator + indexName + MAPPING_EXTENSION);
		if (indexMapping.exists()) {
			try {
				ZuliaIndex.IndexMapping.Builder indexMappingBuilder = ZuliaIndex.IndexMapping.newBuilder();
				JsonFormat.parser().merge(new FileReader(indexMapping), indexMappingBuilder);
				return indexMappingBuilder.build();
			}
			catch (Exception e) {
				LOG.log(Level.SEVERE, e.getMessage(), e);
			}
		}

		return null;
	}

	@Override
	public void storeIndexMapping(ZuliaIndex.IndexMapping indexMapping) {
		try {
			JsonFormat.Printer printer = JsonFormat.printer();
			String indexMappingJson = printer.print(indexMapping);
			writeFile(indexMappingJson, indexMapping.getIndexName() + MAPPING_EXTENSION);
		}
		catch (Exception e) {
			LOG.log(Level.SEVERE, e.getMessage(), e);
		}

	}

	private void writeFile(String json, String filename) throws IOException {
		File file = new File(baseDir + File.separator + filename);
		try (FileWriter fileWriter = new FileWriter(file)) {
			fileWriter.write(json);
		}
	}
}

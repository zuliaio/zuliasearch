package io.zulia.server.config.single;

import com.google.protobuf.util.JsonFormat;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.IndexAlias;
import io.zulia.message.ZuliaIndex.IndexShardMapping;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.exceptions.IndexConfigDoesNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class FSIndexService implements IndexService {

	private static final Logger LOG = LoggerFactory.getLogger(FSIndexService.class);
	private static final String INDEX_EXTENSION = "_index.json";
	private static final String MAPPING_EXTENSION = "_mapping.json";
	private static final String ALIAS_EXTENSION = "_alias.json";
	private static final String SETTINGS_DIR = "settings";
	private final String baseDir;

	public FSIndexService(ZuliaConfig zuliaConfig) {
		// create the base dir
		baseDir = zuliaConfig.getDataPath() + File.separator + SETTINGS_DIR;
		try {
			Files.createDirectory(Paths.get(baseDir));
		}
		catch (FileAlreadyExistsException e1) {
			// skip if the base dir exists.
		}
		catch (IOException e) {
			LOG.error(e.getMessage(), e);
			throw new RuntimeException("Failed to create the base directory to store index settings.");
		}

	}

	@Override
	public List<ZuliaIndex.IndexSettings> getIndexes() throws Exception {

		if (Paths.get(baseDir).toFile().exists()) {
			List<ZuliaIndex.IndexSettings> indexes = new ArrayList<>();
			for (File file : Objects.requireNonNull(Paths.get(baseDir).toFile().listFiles())) {
				if (file.getName().endsWith(INDEX_EXTENSION)) {
					indexes.add(getIndexSettings(file));
				}
			}
			return indexes;
		}

		return Collections.emptyList();
	}

	@Override
	public ZuliaIndex.IndexSettings getIndex(String indexName) throws Exception {
		File indexSettingsFile = new File(baseDir + File.separator + indexName + INDEX_EXTENSION);
		if (indexSettingsFile.exists()) {
			return getIndexSettings(indexSettingsFile);
		}
		else {
			return null;
		}
	}

	@Override
	public void storeIndex(ZuliaIndex.IndexSettings indexSettings) throws Exception {
		JsonFormat.Printer printer = JsonFormat.printer();
		String indexSettingsJson = printer.print(indexSettings);
		writeFile(indexSettingsJson, indexSettings.getIndexName() + INDEX_EXTENSION);
	}

	@Override
	public void removeIndex(String indexName) throws Exception {
		Files.deleteIfExists(Paths.get(baseDir + File.separator + indexName + INDEX_EXTENSION));
	}

	@Override
	public List<IndexShardMapping> getIndexShardMappings() throws Exception {

		if (Paths.get(baseDir).toFile().exists()) {
			List<IndexShardMapping> indexShardMappings = new ArrayList<>();
			for (File file : Objects.requireNonNull(Paths.get(baseDir).toFile().listFiles())) {
				if (file.getName().endsWith(MAPPING_EXTENSION)) {
					indexShardMappings.add(getIndexShardMapping(file));
				}
			}
			return indexShardMappings;
		}

		return Collections.emptyList();

	}

	@Override
	public IndexShardMapping getIndexShardMapping(String indexName) throws Exception {
		return getIndexShardMapping(new File(baseDir + File.separator + indexName + MAPPING_EXTENSION));
	}

	@Override
	public void storeIndexShardMapping(IndexShardMapping indexShardMapping) throws IOException {
		JsonFormat.Printer printer = JsonFormat.printer();
		String indexMappingJson = printer.print(indexShardMapping);
		writeFile(indexMappingJson, indexShardMapping.getIndexName() + MAPPING_EXTENSION);
	}

	@Override
	public void removeIndexShardMapping(String indexName) throws Exception {
		Files.deleteIfExists(Paths.get(baseDir + File.separator + indexName + MAPPING_EXTENSION));
	}

	@Override
	public List<IndexAlias> getIndexAliases() throws Exception {
		if (Paths.get(baseDir).toFile().exists()) {
			List<IndexAlias> indexAliases = new ArrayList<>();
			for (File file : Objects.requireNonNull(Paths.get(baseDir).toFile().listFiles())) {
				if (file.getName().endsWith(ALIAS_EXTENSION)) {
					indexAliases.add(getIndexAlias(file));
				}
			}
			return indexAliases;
		}

		return Collections.emptyList();
	}

	public IndexAlias getIndexAlias(String indexAlias) throws Exception {
		return getIndexAlias(new File(baseDir + File.separator + indexAlias + ALIAS_EXTENSION));
	}

	@Override
	public void storeIndexAlias(IndexAlias indexAlias) throws Exception {
		JsonFormat.Printer printer = JsonFormat.printer();
		String indexAliasJson = printer.print(indexAlias);
		writeFile(indexAliasJson, indexAlias.getAliasName() + ALIAS_EXTENSION);
	}

	@Override
	public void removeIndexAlias(String indexAlias) throws Exception {
		Files.deleteIfExists(Paths.get(baseDir + File.separator + indexAlias + ALIAS_EXTENSION));
	}

	private void writeFile(String json, String filename) throws IOException {
		File file = new File(baseDir + File.separator + filename);
		try (FileWriter fileWriter = new FileWriter(file)) {
			fileWriter.write(json);
		}
	}

	private ZuliaIndex.IndexSettings getIndexSettings(File indexSettingsFile) throws IOException {
		if (!indexSettingsFile.exists()) {
			throw new IndexConfigDoesNotExistException(indexSettingsFile.getName());
		}

		JsonFormat.Parser parser = JsonFormat.parser();
		ZuliaIndex.IndexSettings.Builder indexSettingsBuilder = ZuliaIndex.IndexSettings.newBuilder();
		parser.merge(new FileReader(indexSettingsFile), indexSettingsBuilder);
		return indexSettingsBuilder.build();
	}

	private IndexShardMapping getIndexShardMapping(File indexMappingFile) throws IOException {
		if (!indexMappingFile.exists()) {
			throw new IndexConfigDoesNotExistException(indexMappingFile.getName());
		}

		IndexShardMapping.Builder indexMappingBuilder = IndexShardMapping.newBuilder();
		JsonFormat.parser().merge(new FileReader(indexMappingFile), indexMappingBuilder);
		return indexMappingBuilder.build();
	}

	private IndexAlias getIndexAlias(File indexAliasFile) throws IOException {
		if (!indexAliasFile.exists()) {
			throw new IndexConfigDoesNotExistException(indexAliasFile.getName());
		}

		IndexAlias.Builder indexAliasBuilder = IndexAlias.newBuilder();
		JsonFormat.parser().merge(new FileReader(indexAliasFile), indexAliasBuilder);
		return indexAliasBuilder.build();
	}

}

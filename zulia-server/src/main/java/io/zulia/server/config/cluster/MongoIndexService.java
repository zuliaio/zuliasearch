package io.zulia.server.config.cluster;

import com.google.protobuf.util.JsonFormat;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import io.zulia.message.ZuliaIndex.IndexMapping;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.server.config.IndexService;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoIndexService implements IndexService {

	private static final String MAPPING = "mapping";
	private final MongoCollection<Document> settingsCollection;
	private final MongoCollection<Document> mappingCollection;

	public MongoIndexService(MongoClient mongoClient) {
		settingsCollection = mongoClient.getDatabase(SETTINGS).getCollection(SETTINGS);
		mappingCollection = mongoClient.getDatabase(SETTINGS).getCollection(MAPPING);
	}

	@Override
	public List<IndexSettings> getIndexes() throws Exception {

		List<IndexSettings> indexSettings = new ArrayList<>();
		for (Document doc : settingsCollection.find()) {
			String indexSettingsJson = doc.getString("indexSettings");
			IndexSettings.Builder builder = IndexSettings.newBuilder();
			JsonFormat.parser().merge(indexSettingsJson, builder);
			indexSettings.add(builder.build());
		}

		if (!indexSettings.isEmpty()) {
			return indexSettings;
		}

		return null;
	}

	@Override
	public void createIndex(IndexSettings indexSettings) throws Exception {

		Document indexSettingsDoc = new Document("_id", indexSettings.getIndexName()).append("indexSettings", JsonFormat.printer().print(indexSettings));

		settingsCollection.replaceOne(new Document("_id", indexSettings.getIndexName()), indexSettingsDoc, new UpdateOptions().upsert(true));

	}

	@Override
	public void removeIndex(String indexName) throws Exception {
		settingsCollection.deleteOne(new Document("_id", indexName));
		mappingCollection.deleteOne(new Document("_id", indexName));
	}

	@Override
	public List<IndexMapping> getIndexMappings() throws Exception {

		List<IndexMapping> indexMappings = new ArrayList<>();
		for (Document doc : mappingCollection.find()) {

			String indexMappingJson = doc.getString("indexMapping");
			IndexMapping.Builder builder = IndexMapping.newBuilder();
			JsonFormat.parser().merge(indexMappingJson, builder);
			
			indexMappings.add(builder.build());

		}

		if (!indexMappings.isEmpty()) {
			return indexMappings;
		}

		return null;

	}

	@Override
	public IndexMapping getIndexMapping(String indexName) throws Exception {
		Document doc = mappingCollection.find(new Document("_id", indexName)).first();

		if (doc != null) {
			String indexMappingJson = doc.getString("indexMapping");
			IndexMapping.Builder builder = IndexMapping.newBuilder();
			JsonFormat.parser().merge(indexMappingJson, builder);
			return builder.build();
		}

		return null;

	}

	@Override
	public void storeIndexMapping(IndexMapping indexMapping) throws Exception {
		Document indexMappingDoc = new Document("_id", indexMapping.getIndexName()).append("indexMapping", JsonFormat.printer().print(indexMapping));

		mappingCollection.replaceOne(new Document("_id", indexMapping.getIndexName()), indexMappingDoc, new UpdateOptions().upsert(true));
	}
}

package io.zulia.server.config.cluster;import com.google.protobuf.InvalidProtocolBufferException;import com.google.protobuf.util.JsonFormat;import com.mongodb.client.MongoClient;import com.mongodb.client.MongoCollection;import com.mongodb.client.model.ReplaceOptions;import io.zulia.message.ZuliaIndex.IndexAlias;import io.zulia.message.ZuliaIndex.IndexMapping;import io.zulia.message.ZuliaIndex.IndexSettings;import io.zulia.server.config.IndexService;import io.zulia.server.exceptions.IndexConfigDoesNotExistException;import org.bson.Document;import java.util.ArrayList;import java.util.List;public class MongoIndexService implements IndexService {	private static final String ID = "_id";	private static final String SETTINGS = "settings";	private static final String MAPPING = "mapping";	private static final String ALIAS = "alias";	private static final String INDEX_SETTINGS = "indexSettings";	private static final String INDEX_MAPPING = "indexMapping";	private static final String INDEX_ALIAS = "indexAlias";	private final MongoCollection<Document> settingsCollection;	private final MongoCollection<Document> mappingCollection;	private final MongoCollection<Document> aliasCollection;	public MongoIndexService(MongoClient mongoClient, String clusterName) {		settingsCollection = mongoClient.getDatabase(clusterName).getCollection(SETTINGS);		mappingCollection = mongoClient.getDatabase(clusterName).getCollection(MAPPING);		aliasCollection = mongoClient.getDatabase(clusterName).getCollection(ALIAS);	}	@Override	public List<IndexSettings> getIndexes() throws Exception {		List<IndexSettings> indexSettings = new ArrayList<>();		for (Document doc : settingsCollection.find()) {			indexSettings.add(getIndexSettingsFromDoc(doc));		}		return indexSettings;	}	@Override	public IndexSettings getIndex(String indexName) throws Exception {		Document doc = settingsCollection.find(new Document(ID, indexName)).first();		if (doc == null) {			return null;		}		return getIndexSettingsFromDoc(doc);	}	@Override	public void storeIndex(IndexSettings indexSettings) throws Exception {		Document indexSettingsDoc = new Document(ID, indexSettings.getIndexName())				.append(INDEX_SETTINGS, Document.parse(JsonFormat.printer().print(indexSettings)));		settingsCollection.replaceOne(new Document(ID, indexSettings.getIndexName()), indexSettingsDoc, new ReplaceOptions().upsert(true));	}	@Override	public void removeIndex(String indexName) {		settingsCollection.deleteOne(new Document(ID, indexName));	}	@Override	public List<IndexMapping> getIndexMappings() throws Exception {		List<IndexMapping> indexMappings = new ArrayList<>();		for (Document doc : mappingCollection.find()) {			indexMappings.add(getIndexMappingFromDoc(doc));		}		return indexMappings;	}	@Override	public IndexMapping getIndexMapping(String indexName) throws Exception {		Document doc = mappingCollection.find(new Document(ID, indexName)).first();		if (doc == null) {			throw new IndexConfigDoesNotExistException(indexName);		}		return getIndexMappingFromDoc(doc);	}	@Override	public void storeIndexMapping(IndexMapping indexMapping) throws Exception {		Document indexMappingDoc = new Document(ID, indexMapping.getIndexName())				.append(INDEX_MAPPING, Document.parse(JsonFormat.printer().print(indexMapping)));		mappingCollection.replaceOne(new Document(ID, indexMapping.getIndexName()), indexMappingDoc, new ReplaceOptions().upsert(true));	}	@Override	public void removeIndexMapping(String indexName) {		mappingCollection.deleteOne(new Document(ID, indexName));	}	@Override	public List<IndexAlias> getIndexAliases() throws Exception {		List<IndexAlias> indexAliases = new ArrayList<>();		for (Document doc : aliasCollection.find()) {			indexAliases.add(getIndexAliasFromDoc(doc));		}		return indexAliases;	}	@Override	public IndexAlias getIndexAlias(String indexAlias) throws Exception {		Document doc = aliasCollection.find(new Document(ID, indexAlias)).first();		if (doc == null) {			return null;		}		return getIndexAliasFromDoc(doc);	}	@Override	public void storeIndexAlias(IndexAlias indexAlias) throws Exception {		Document indexAliasDoc = new Document(ID, indexAlias.getAliasName()).append(INDEX_ALIAS, Document.parse(JsonFormat.printer().print(indexAlias)));		aliasCollection.replaceOne(new Document(ID, indexAlias.getAliasName()), indexAliasDoc, new ReplaceOptions().upsert(true));	}	@Override	public void removeIndexAlias(String indexAlias) throws Exception {		aliasCollection.deleteOne(new Document(ID, indexAlias));	}	private IndexSettings getIndexSettingsFromDoc(Document doc) throws InvalidProtocolBufferException {		Document indexSettingsDoc = (Document) doc.get(INDEX_SETTINGS);		IndexSettings.Builder builder = IndexSettings.newBuilder();		JsonFormat.parser().merge(indexSettingsDoc.toJson(), builder);		return builder.build();	}	private IndexMapping getIndexMappingFromDoc(Document doc) throws InvalidProtocolBufferException {		Document mappingDoc = (Document) doc.get(INDEX_MAPPING);		IndexMapping.Builder builder = IndexMapping.newBuilder();		JsonFormat.parser().merge(mappingDoc.toJson(), builder);		return builder.build();	}	private IndexAlias getIndexAliasFromDoc(Document doc) throws InvalidProtocolBufferException {		Document indexAliasDoc = (Document) doc.get(INDEX_ALIAS);		IndexAlias.Builder builder = IndexAlias.newBuilder();		JsonFormat.parser().merge(indexAliasDoc.toJson(), builder);		return builder.build();	}}
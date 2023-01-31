package io.zulia.server.config.cluster;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import io.zulia.message.ZuliaIndex.IndexAlias;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaIndex.IndexShardMapping;
import io.zulia.server.config.IndexService;
import io.zulia.server.exceptions.IndexConfigDoesNotExistException;
import org.bson.Document;
import org.bson.types.Binary;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class MongoIndexService implements IndexService {

    private static final String ID = "_id";
    private static final String SETTINGS = "settings";
    private static final String MAPPING = "mapping";

    private static final String ALIAS = "alias";

    private static final String COMPRESSED_INDEX_SETTINGS = "compressedIndexSettings";

    private static final String INDEX_SETTINGS = "indexSettings";
    private static final String INDEX_MAPPING = "indexMapping";
    private static final String INDEX_ALIAS = "indexAlias";

    private final MongoCollection<Document> settingsCollection;
    private final MongoCollection<Document> mappingCollection;
    private final MongoCollection<Document> aliasCollection;

    public MongoIndexService(MongoClient mongoClient, String clusterName) {
        settingsCollection = mongoClient.getDatabase(clusterName).getCollection(SETTINGS);
        mappingCollection = mongoClient.getDatabase(clusterName).getCollection(MAPPING);
        aliasCollection = mongoClient.getDatabase(clusterName).getCollection(ALIAS);
    }

    @Override
    public List<IndexSettings> getIndexes() throws Exception {

        List<IndexSettings> indexSettings = new ArrayList<>();
        for (Document doc : settingsCollection.find()) {
            indexSettings.add(getIndexSettingsFromDoc(doc));
        }

        return indexSettings;
    }

    @Override
    public IndexSettings getIndex(String indexName) throws Exception {
        Document doc = settingsCollection.find(new Document(ID, indexName)).first();
        if (doc == null) {
            return null;
        }
        return getIndexSettingsFromDoc(doc);
    }

    @Override
    public void storeIndex(IndexSettings indexSettings) throws Exception {

        String indexJson = JsonFormat.printer().print(indexSettings);

        Document indexSettingsDoc;
        //mongodb limit is 16MB but might not be exact translation from JSON to BSON so let's do 10MB
        if (indexJson.length() > (10 * 1024 * 1024)) {
            indexSettingsDoc = new Document(ID, indexSettings.getIndexName()).append(COMPRESSED_INDEX_SETTINGS, compressJson(indexJson));
        } else {
            indexSettingsDoc = new Document(ID, indexSettings.getIndexName()).append(INDEX_SETTINGS, Document.parse(indexJson));
        }
        settingsCollection.replaceOne(new Document(ID, indexSettings.getIndexName()), indexSettingsDoc, new ReplaceOptions().upsert(true));

    }

    private static byte[] compressJson(String indexJson) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (SnappyOutputStream sos = new SnappyOutputStream(out)) {
            sos.write(indexJson.getBytes(StandardCharsets.UTF_8));
        }
        return out.toByteArray();
    }

    @Override
    public void removeIndex(String indexName) {
        settingsCollection.deleteOne(new Document(ID, indexName));
    }

    @Override
    public List<IndexShardMapping> getIndexShardMappings() throws Exception {

        List<IndexShardMapping> indexShardMappings = new ArrayList<>();
        for (Document doc : mappingCollection.find()) {
            indexShardMappings.add(getIndexMappingFromDoc(doc));
        }

        return indexShardMappings;

    }

    @Override
    public IndexShardMapping getIndexShardMapping(String indexName) throws Exception {
        Document doc = mappingCollection.find(new Document(ID, indexName)).first();
        if (doc == null) {
            throw new IndexConfigDoesNotExistException(indexName);
        }
        return getIndexMappingFromDoc(doc);

    }

    @Override
    public void storeIndexShardMapping(IndexShardMapping indexShardMapping) throws Exception {
        Document indexMappingDoc = new Document(ID, indexShardMapping.getIndexName()).append(INDEX_MAPPING,
                Document.parse(JsonFormat.printer().print(indexShardMapping)));
        mappingCollection.replaceOne(new Document(ID, indexShardMapping.getIndexName()), indexMappingDoc, new ReplaceOptions().upsert(true));
    }

    @Override
    public void removeIndexShardMapping(String indexName) {
        mappingCollection.deleteOne(new Document(ID, indexName));
    }

    @Override
    public List<IndexAlias> getIndexAliases() throws Exception {
        List<IndexAlias> indexAliases = new ArrayList<>();
        for (Document doc : aliasCollection.find()) {
            indexAliases.add(getIndexAliasFromDoc(doc));
        }
        return indexAliases;

    }

    @Override
    public IndexAlias getIndexAlias(String indexAlias) throws Exception {
        Document doc = aliasCollection.find(new Document(ID, indexAlias)).first();
        if (doc == null) {
            return null;
        }
        return getIndexAliasFromDoc(doc);
    }

    @Override
    public void storeIndexAlias(IndexAlias indexAlias) throws Exception {
        Document indexAliasDoc = new Document(ID, indexAlias.getAliasName()).append(INDEX_ALIAS, Document.parse(JsonFormat.printer().print(indexAlias)));
        aliasCollection.replaceOne(new Document(ID, indexAlias.getAliasName()), indexAliasDoc, new ReplaceOptions().upsert(true));
    }

    @Override
    public void removeIndexAlias(String indexAlias) {
        aliasCollection.deleteOne(new Document(ID, indexAlias));
    }

    private IndexSettings getIndexSettingsFromDoc(Document doc) throws IOException {
        Document indexSettingsDoc = (Document) doc.get(INDEX_SETTINGS);

        if (indexSettingsDoc == null) {
            Binary bytes = doc.get(COMPRESSED_INDEX_SETTINGS, Binary.class);
            try (SnappyInputStream snappyInputStream = new SnappyInputStream(new ByteArrayInputStream(bytes.getData()))) {
                String json = new String(snappyInputStream.readAllBytes(), StandardCharsets.UTF_8);
                indexSettingsDoc = Document.parse(json);
            }
        }

        IndexSettings.Builder builder = IndexSettings.newBuilder();
        JsonFormat.parser().merge(indexSettingsDoc.toJson(), builder);
        return builder.build();
    }

    private IndexShardMapping getIndexMappingFromDoc(Document doc) throws InvalidProtocolBufferException {
        Document mappingDoc = (Document) doc.get(INDEX_MAPPING);
        IndexShardMapping.Builder builder = IndexShardMapping.newBuilder();
        JsonFormat.parser().merge(mappingDoc.toJson(), builder);
        return builder.build();
    }

    private IndexAlias getIndexAliasFromDoc(Document doc) throws InvalidProtocolBufferException {
        Document indexAliasDoc = (Document) doc.get(INDEX_ALIAS);
        IndexAlias.Builder builder = IndexAlias.newBuilder();
        JsonFormat.parser().merge(indexAliasDoc.toJson(), builder);
        return builder.build();
    }

}

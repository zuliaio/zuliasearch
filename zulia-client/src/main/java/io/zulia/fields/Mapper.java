package io.zulia.fields;

import io.zulia.client.command.CreateIndex;
import io.zulia.client.command.Store;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.annotations.*;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.util.AnnotationUtil;
import org.bson.Document;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Mapper<T> extends GsonDocumentMapper<T> {

    private final Class<T> clazz;

    private final FieldConfigMapper<T> fieldConfigMapper;

    private final UniqueIdFieldInfo<T> uniqueIdField;

    private final List<DefaultSearchFieldInfo<T>> defaultSearchFields = new ArrayList<>();

    private final Settings settings;

    public Mapper(Class<T> clazz) {
        super(clazz);

        this.clazz = clazz;

        this.fieldConfigMapper = new FieldConfigMapper<>(clazz, "");

        HashSet<String> fields = new HashSet<>();

        List<Field> allFields = AnnotationUtil.getNonStaticFields(clazz, true);

        UniqueIdFieldInfo uf = null;
        for (Field f : allFields) {
            f.setAccessible(true);

            String fieldName = f.getName();

            fieldConfigMapper.setupField(f);

            if (f.isAnnotationPresent(UniqueId.class)) {

                if (f.isAnnotationPresent(AsField.class)) {
                    throw new RuntimeException("Cannot use AsField with UniqueId on field <" + f.getName() + "> for class <" + clazz.getSimpleName()
                            + ">.  Unique id always stored as _id.");
                }
                if (f.isAnnotationPresent(Embedded.class)) {
                    throw new RuntimeException(
                            "Cannot use Embedded with UniqueId with on field <" + f.getName() + "> for class <" + clazz.getSimpleName() + ">");
                }

                @SuppressWarnings("unused") UniqueId uniqueId = f.getAnnotation(UniqueId.class);

                if (uf == null) {
                    uf = new UniqueIdFieldInfo<>(f, fieldName);

                    if (!String.class.equals(f.getType())) {
                        throw new RuntimeException("Unique id field must be a String in class <" + clazz.getSimpleName() + ">");
                    }

                } else {
                    throw new RuntimeException("Cannot define two unique id fields for class <" + clazz.getSimpleName() + ">");
                }

            }

            if (f.isAnnotationPresent(DefaultSearch.class)) {

                if (!f.isAnnotationPresent(Indexed.class) && !f.isAnnotationPresent(IndexedFields.class)) {
                    throw new RuntimeException("DefaultSearch must be on Indexed field <" + f.getName() + "> for class <" + clazz.getSimpleName() + ">");
                }

                @SuppressWarnings("unused") DefaultSearch defaultSearch = f.getAnnotation(DefaultSearch.class);

                defaultSearchFields.add(new DefaultSearchFieldInfo<>(f, fieldName));

            }

            if (fields.contains(fieldName)) {
                throw new RuntimeException("Duplicate field name <" + fieldName + ">");
            }
            fields.add(fieldName);

        }
        if (uf == null) {
            throw new RuntimeException("A unique id field must be defined for class <" + clazz.getSimpleName() + ">");
        }

        this.uniqueIdField = uf;

        if (clazz.isAnnotationPresent(Settings.class)) {
            settings = clazz.getAnnotation(Settings.class);
        } else {
            settings = null;
        }

    }

    public CreateIndex createOrUpdateIndex() {

        if (settings == null) {
            throw new RuntimeException("No Settings annotation for class <" + clazz.getSimpleName() + ">");
        }

        ClientIndexConfig indexConfig = new ClientIndexConfig();

        for (DefaultSearchFieldInfo<T> defaultSearchField : defaultSearchFields) {
            indexConfig.addDefaultSearchField(defaultSearchField.getFieldName());
        }

        indexConfig.setIndexName(settings.indexName());
        indexConfig.setNumberOfShards(settings.numberOfShards());
        indexConfig.setRequestFactor(settings.requestFactor());
        indexConfig.setMinShardRequest(settings.minSeqmentRequest());
        indexConfig.setIdleTimeWithoutCommit(settings.idleTimeWithoutCommit());
        indexConfig.setShardCommitInterval(settings.shardCommitInterval());
        indexConfig.setShardTolerance(settings.shardTolerance());
        indexConfig.setShardQueryCacheSize(settings.shardQueryCacheSize());
        indexConfig.setShardQueryCacheMaxAmount(settings.shardQueryCacheMaxAmount());

        for (FieldConfig fieldConfig : fieldConfigMapper.getFieldConfigs()) {
            indexConfig.addFieldConfig(fieldConfig);
        }

        return new CreateIndex(indexConfig);
    }

    public Class<T> getClazz() {
        return clazz;
    }

    public Store createStore(T object) throws Exception {
        if (settings == null) {
            throw new RuntimeException("No Settings annotation for class <" + clazz.getSimpleName() + ">");
        }
        return createStore(settings.indexName(), object);
    }

    public Store createStore(String indexName, T object) throws Exception {
        ResultDocBuilder rd = toResultDocumentBuilder(object);
        Store store = new Store(rd.getUniqueId(), indexName);
        store.setResultDocument(rd);
        return store;
    }

    public ResultDocBuilder toResultDocumentBuilder(T object) throws Exception {
        String uniqueId = uniqueIdField.build(object);
        Document document = toDocument(object);
        ResultDocBuilder resultDocumentBuilder = new ResultDocBuilder();
        resultDocumentBuilder.setDocument(document).setUniqueId(uniqueId);
        return resultDocumentBuilder;
    }

}

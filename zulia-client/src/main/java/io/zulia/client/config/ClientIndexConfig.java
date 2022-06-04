package io.zulia.client.config;

import io.zulia.fields.FieldConfigBuilder;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import static io.zulia.message.ZuliaBase.Similarity;
import static io.zulia.message.ZuliaIndex.AnalyzerSettings;
import static io.zulia.message.ZuliaIndex.FieldConfig;
import static io.zulia.message.ZuliaIndex.IndexSettings;

public class ClientIndexConfig {

	private List<String> defaultSearchFields = Collections.emptyList();
	private Double requestFactor;
	private Integer minShardRequest;
	private Integer numberOfShards;
	private String indexName;
	private Integer idleTimeWithoutCommit;
	private Integer shardCommitInterval;
	private Double shardTolerance;
	private Integer shardQueryCacheSize;
	private Integer shardQueryCacheMaxAmount;
	private Integer indexWeight;
	private Integer ramBufferMB;
	private Integer numberOfReplicas;

	private TreeMap<String, FieldConfig> fieldMap;
	private TreeMap<String, AnalyzerSettings> analyzerSettingsMap;

	private Document meta;

	public ClientIndexConfig() {
		this.fieldMap = new TreeMap<>();
		this.analyzerSettingsMap = new TreeMap<>();
	}

	public ClientIndexConfig addDefaultSearchField(String defaultSearchField) {
		if (defaultSearchFields.isEmpty()) {
			defaultSearchFields = new ArrayList<>();
		}
		defaultSearchFields.add(defaultSearchField);
		return this;
	}

	public List<String> getDefaultSearchFields() {
		return defaultSearchFields;
	}

	public ClientIndexConfig setDefaultSearchFields(List<String> defaultSearchFields) {
		this.defaultSearchFields = defaultSearchFields;
		return this;
	}

	public Double getRequestFactor() {
		return requestFactor;
	}

	public ClientIndexConfig setRequestFactor(Double requestFactor) {
		this.requestFactor = requestFactor;
		return this;
	}

	public Integer getMinShardRequest() {
		return minShardRequest;
	}

	public ClientIndexConfig setMinShardRequest(Integer minShardRequest) {
		this.minShardRequest = minShardRequest;
		return this;
	}

	public Integer getNumberOfShards() {
		return numberOfShards;
	}

	public Integer getRamBufferMB() {
		return ramBufferMB;
	}

	public void setRamBufferMB(Integer ramBufferMB) {
		this.ramBufferMB = ramBufferMB;
	}

	public ClientIndexConfig setNumberOfShards(Integer numberOfShards) {
		this.numberOfShards = numberOfShards;
		return this;
	}

	public String getIndexName() {
		return indexName;
	}

	public ClientIndexConfig setIndexName(String indexName) {
		this.indexName = indexName;
		return this;
	}

	public Integer getIdleTimeWithoutCommit() {
		return idleTimeWithoutCommit;
	}

	public ClientIndexConfig setIdleTimeWithoutCommit(Integer idleTimeWithoutCommit) {
		this.idleTimeWithoutCommit = idleTimeWithoutCommit;
		return this;
	}

	public Integer getShardCommitInterval() {
		return shardCommitInterval;
	}

	public ClientIndexConfig setShardCommitInterval(Integer shardCommitInterval) {
		this.shardCommitInterval = shardCommitInterval;
		return this;
	}

	public Double getShardTolerance() {
		return shardTolerance;
	}

	public ClientIndexConfig setShardTolerance(Double shardTolerance) {
		this.shardTolerance = shardTolerance;
		return this;
	}

	public Integer getShardQueryCacheSize() {
		return shardQueryCacheSize;
	}

	public void setShardQueryCacheSize(Integer shardQueryCacheSize) {
		this.shardQueryCacheSize = shardQueryCacheSize;
	}

	public Integer getShardQueryCacheMaxAmount() {
		return shardQueryCacheMaxAmount;
	}

	public void setShardQueryCacheMaxAmount(Integer shardQueryCacheMaxAmount) {
		this.shardQueryCacheMaxAmount = shardQueryCacheMaxAmount;
	}

	public void addFieldConfig(FieldConfigBuilder FieldConfigBuilder) {
		addFieldConfig(FieldConfigBuilder.build());
	}

	public void addFieldConfig(FieldConfig fieldConfig) {
		this.fieldMap.put(fieldConfig.getStoredFieldName(), fieldConfig);
	}

	public FieldConfig getFieldConfig(String fieldName) {
		return this.fieldMap.get(fieldName);
	}

	public Integer getIndexWeight() {
		return indexWeight;
	}

	public void setIndexWeight(Integer indexWeight) {
		this.indexWeight = indexWeight;
	}

	public Integer getNumberOfReplicas() {
		return numberOfReplicas;
	}

	public void setNumberOfReplicas(Integer numberOfReplicas) {
		this.numberOfReplicas = numberOfReplicas;
	}

	public void addAnalyzerSetting(String name, AnalyzerSettings.Tokenizer tokenizer, Iterable<AnalyzerSettings.Filter> filterList, Similarity similarity) {

		AnalyzerSettings.Builder analyzerSettings = AnalyzerSettings.newBuilder();
		analyzerSettings.setName(name);
		if (tokenizer != null) {
			analyzerSettings.setTokenizer(tokenizer);
		}
		if (filterList != null) {
			analyzerSettings.addAllFilter(filterList);
		}
		if (similarity != null) {
			analyzerSettings.setSimilarity(similarity);
		}

		addAnalyzerSetting(analyzerSettings.build());
	}

	public void addAnalyzerSetting(AnalyzerSettings analyzerSettings) {
		analyzerSettingsMap.put(analyzerSettings.getName(), analyzerSettings);
	}

	public TreeMap<String, FieldConfig> getFieldConfigMap() {
		return fieldMap;
	}

	public void setMeta(Document meta) {
		this.meta = meta;
	}

	public Document getMeta() {
		return meta;
	}

	public IndexSettings getIndexSettings() {
		IndexSettings.Builder isb = IndexSettings.newBuilder();

		if (numberOfShards != null) {
			isb.setNumberOfShards(numberOfShards);
		}

		if (indexName != null) {
			isb.setIndexName(indexName);
		}

		if (defaultSearchFields != null) {
			isb.addAllDefaultSearchField(defaultSearchFields);
		}

		if (requestFactor != null) {
			isb.setRequestFactor(requestFactor);
		}

		if (minShardRequest != null) {
			isb.setMinShardRequest(minShardRequest);
		}

		if (shardCommitInterval != null) {
			isb.setShardCommitInterval(shardCommitInterval);
		}

		if (idleTimeWithoutCommit != null) {
			isb.setIdleTimeWithoutCommit(idleTimeWithoutCommit);
		}

		if (shardTolerance != null) {
			isb.setShardTolerance(shardTolerance);
		}

		if (indexWeight != null) {
			isb.setIndexWeight(indexWeight);
		}

		if (ramBufferMB != null) {
			isb.setRamBufferMB(ramBufferMB);
		}

		for (String fieldName : fieldMap.keySet()) {
			FieldConfig fieldConfig = fieldMap.get(fieldName);
			isb.addFieldConfig(fieldConfig);
		}

		for (String analyzerName : analyzerSettingsMap.keySet()) {
			isb.addAnalyzerSettings(analyzerSettingsMap.get(analyzerName));
		}

		if (meta != null) {
			isb.setMeta(ZuliaUtil.mongoDocumentToByteString(meta));
		}

		return isb.build();
	}

	public void configure(IndexSettings indexSettings) {
		this.indexName = indexSettings.getIndexName();
		this.numberOfShards = indexSettings.getNumberOfShards();
		this.numberOfReplicas = indexSettings.getNumberOfReplicas();
		this.defaultSearchFields = indexSettings.getDefaultSearchFieldList();

		this.analyzerSettingsMap = new TreeMap<>();
		for (AnalyzerSettings analyzerSettings : indexSettings.getAnalyzerSettingsList()) {
			analyzerSettingsMap.put(analyzerSettings.getName(), analyzerSettings);
		}

		this.fieldMap = new TreeMap<>();
		for (FieldConfig fc : indexSettings.getFieldConfigList()) {
			fieldMap.put(fc.getStoredFieldName(), fc);
		}

		this.requestFactor = indexSettings.getRequestFactor();
		this.minShardRequest = indexSettings.getMinShardRequest();
		this.shardTolerance = indexSettings.getShardTolerance();
		this.shardQueryCacheSize = indexSettings.getShardQueryCacheSize();
		this.shardQueryCacheMaxAmount = indexSettings.getShardQueryCacheMaxAmount();
		this.idleTimeWithoutCommit = indexSettings.getIdleTimeWithoutCommit();
		this.shardCommitInterval = indexSettings.getShardCommitInterval();

		this.indexWeight = indexSettings.getIndexWeight();
		this.ramBufferMB = indexSettings.getRamBufferMB();

		this.meta = ZuliaUtil.byteStringToMongoDocument(indexSettings.getMeta());

	}

	public static ClientIndexConfig fromIndexSettings(IndexSettings indexSettings) {
		ClientIndexConfig ic = new ClientIndexConfig();
		ic.configure(indexSettings);
		return ic;
	}

}

package io.zulia.client.config;

import io.zulia.fields.FieldConfigBuilder;

import java.util.TreeMap;

import static io.zulia.message.ZuliaBase.Similarity;
import static io.zulia.message.ZuliaIndex.AnalyzerSettings;
import static io.zulia.message.ZuliaIndex.FieldConfig;
import static io.zulia.message.ZuliaIndex.IndexSettings;

public class ClientIndexConfig {

	private String defaultSearchField;
	private Double requestFactor;
	private Integer minShardRequest;
	private Integer numberOfShards;
	private String indexName;
	private Integer idleTimeWithoutCommit;
	private Integer shardCommitInterval;
	private Double shardTolerance;
	private Integer shardQueryCacheSize;
	private Integer shardQueryCacheMaxAmount;

	private TreeMap<String, FieldConfig> fieldMap;
	private TreeMap<String, AnalyzerSettings> analyzerSettingsMap;

	public ClientIndexConfig() {
		this(null);
	}

	public ClientIndexConfig(String defaultSearchField) {
		this.defaultSearchField = defaultSearchField;
		this.fieldMap = new TreeMap<>();
		this.analyzerSettingsMap = new TreeMap<>();
	}

	public String getDefaultSearchField() {
		return defaultSearchField;
	}

	public ClientIndexConfig setDefaultSearchField(String defaultSearchField) {
		this.defaultSearchField = defaultSearchField;
		return this;
	}

	public double getRequestFactor() {
		return requestFactor;
	}

	public ClientIndexConfig setRequestFactor(double requestFactor) {
		this.requestFactor = requestFactor;
		return this;
	}

	public int getMinShardRequest() {
		return minShardRequest;
	}

	public ClientIndexConfig setMinShardRequest(int minShardRequest) {
		this.minShardRequest = minShardRequest;
		return this;
	}

	public int getNumberOfShards() {
		return numberOfShards;
	}

	public ClientIndexConfig setNumberOfShards(int numberOfShards) {
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

	public int getIdleTimeWithoutCommit() {
		return idleTimeWithoutCommit;
	}

	public ClientIndexConfig setIdleTimeWithoutCommit(int idleTimeWithoutCommit) {
		this.idleTimeWithoutCommit = idleTimeWithoutCommit;
		return this;
	}

	public int getShardCommitInterval() {
		return shardCommitInterval;
	}

	public ClientIndexConfig setShardCommitInterval(int shardCommitInterval) {
		this.shardCommitInterval = shardCommitInterval;
		return this;
	}

	public double getShardTolerance() {
		return shardTolerance;
	}

	public ClientIndexConfig setShardTolerance(double shardTolerance) {
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

	public IndexSettings getIndexSettings() {
		IndexSettings.Builder isb = IndexSettings.newBuilder();
		if (defaultSearchField != null) {
			isb.setDefaultSearchField(defaultSearchField);
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

		for (String fieldName : fieldMap.keySet()) {
			FieldConfig fieldConfig = fieldMap.get(fieldName);
			isb.addFieldConfig(fieldConfig);
		}

		for (String analyzerName : analyzerSettingsMap.keySet()) {
			isb.addAnalyzerSettings(analyzerSettingsMap.get(analyzerName));
		}

		return isb.build();
	}

	public void configure(IndexSettings indexSettings) {
		this.defaultSearchField = indexSettings.getDefaultSearchField();
		this.requestFactor = indexSettings.getRequestFactor();
		this.minShardRequest = indexSettings.getMinShardRequest();
		this.shardCommitInterval = indexSettings.getShardCommitInterval();
		this.idleTimeWithoutCommit = indexSettings.getIdleTimeWithoutCommit();
		this.shardTolerance = indexSettings.getShardTolerance();
		this.fieldMap = new TreeMap<>();

		for (FieldConfig fc : indexSettings.getFieldConfigList()) {
			fieldMap.put(fc.getStoredFieldName(), fc);
		}

	}

	public static ClientIndexConfig fromIndexSettings(IndexSettings indexSettings) {
		ClientIndexConfig ic = new ClientIndexConfig();
		ic.configure(indexSettings);
		return ic;
	}

}

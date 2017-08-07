package io.zulia.client.config;

import org.lumongo.cluster.message.LumongoIndex.AnalyzerSettings;
import org.lumongo.cluster.message.LumongoIndex.AnalyzerSettings.Similarity;
import org.lumongo.cluster.message.LumongoIndex.CommitSettings;
import org.lumongo.cluster.message.LumongoIndex.FieldConfig;
import org.lumongo.cluster.message.LumongoIndex.IndexSettings;
import org.lumongo.cluster.message.LumongoIndex.SearchSettings;
import org.lumongo.fields.FieldConfigBuilder;

import java.util.TreeMap;

public class IndexConfig {

	private String indexName;
	private Integer numberOfSegments;

	private String defaultSearchField;

	private Double requestFactor;
	private Integer minSegmentRequest;
	private Double segmentTolerance;

	private Integer idleTimeWithoutCommit;
	private Integer segmentCommitInterval;

	private Integer segmentQueryCacheSize;
	private Integer segmentQueryCacheMaxAmount;

	private TreeMap<String, FieldConfig> fieldMap;
	private TreeMap<String, AnalyzerSettings> analyzerSettingsMap;

	public IndexConfig() {
		this(null);
	}

	public IndexConfig(String defaultSearchField) {
		this.defaultSearchField = defaultSearchField;
		this.fieldMap = new TreeMap<>();
		this.analyzerSettingsMap = new TreeMap<>();
	}

	public String getDefaultSearchField() {
		return defaultSearchField;
	}

	public IndexConfig setDefaultSearchField(String defaultSearchField) {
		this.defaultSearchField = defaultSearchField;
		return this;
	}

	public double getRequestFactor() {
		return requestFactor;
	}

	public IndexConfig setRequestFactor(double requestFactor) {
		this.requestFactor = requestFactor;
		return this;
	}

	public int getMinSegmentRequest() {
		return minSegmentRequest;
	}

	public IndexConfig setMinSegmentRequest(int minSegmentRequest) {
		this.minSegmentRequest = minSegmentRequest;
		return this;
	}

	public int getNumberOfSegments() {
		return numberOfSegments;
	}

	public IndexConfig setNumberOfSegments(int numberOfSegments) {
		this.numberOfSegments = numberOfSegments;
		return this;
	}

	public String getIndexName() {
		return indexName;
	}

	public IndexConfig setIndexName(String indexName) {
		this.indexName = indexName;
		return this;
	}

	public int getIdleTimeWithoutCommit() {
		return idleTimeWithoutCommit;
	}

	public IndexConfig setIdleTimeWithoutCommit(int idleTimeWithoutCommit) {
		this.idleTimeWithoutCommit = idleTimeWithoutCommit;
		return this;
	}

	public int getSegmentCommitInterval() {
		return segmentCommitInterval;
	}

	public IndexConfig setSegmentCommitInterval(int segmentCommitInterval) {
		this.segmentCommitInterval = segmentCommitInterval;
		return this;
	}

	public double getSegmentTolerance() {
		return segmentTolerance;
	}

	public IndexConfig setSegmentTolerance(double segmentTolerance) {
		this.segmentTolerance = segmentTolerance;
		return this;
	}

	public Integer getSegmentQueryCacheSize() {
		return segmentQueryCacheSize;
	}

	public void setSegmentQueryCacheSize(Integer segmentQueryCacheSize) {
		this.segmentQueryCacheSize = segmentQueryCacheSize;
	}

	public Integer getSegmentQueryCacheMaxAmount() {
		return segmentQueryCacheMaxAmount;
	}

	public void setSegmentQueryCacheMaxAmount(Integer segmentQueryCacheMaxAmount) {
		this.segmentQueryCacheMaxAmount = segmentQueryCacheMaxAmount;
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

		SearchSettings.Builder searchSettings = SearchSettings.newBuilder();

		if (defaultSearchField != null) {
			searchSettings.setDefaultSearchField(defaultSearchField);
		}
		if (requestFactor != null) {
			searchSettings.setRequestFactor(requestFactor);
		}
		if (minSegmentRequest != null) {
			searchSettings.setMinSegmentRequest(minSegmentRequest);
		}
		if (segmentTolerance != null) {
			searchSettings.setSegmentTolerance(segmentTolerance);
		}
		if (segmentQueryCacheSize != null) {
			searchSettings.setSegmentQueryCacheSize(segmentQueryCacheSize);
		}

		if (segmentQueryCacheMaxAmount != null) {
			searchSettings.setSegmentQueryCacheMaxAmount(segmentQueryCacheMaxAmount);
		}

		isb.setSearchSettings(searchSettings);

		CommitSettings.Builder commitSettings = CommitSettings.newBuilder();
		if (segmentCommitInterval != null) {
			commitSettings.setSegmentCommitInterval(segmentCommitInterval);
		}
		if (idleTimeWithoutCommit != null) {
			commitSettings.setIdleTimeWithoutCommit(idleTimeWithoutCommit);
		}
		isb.setCommitSettings(commitSettings);

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

		if (indexSettings.hasSearchSettings()) {

			SearchSettings searchSettings = indexSettings.getSearchSettings();
			this.defaultSearchField = searchSettings.getDefaultSearchField();

			this.requestFactor = searchSettings.getRequestFactor();
			this.minSegmentRequest = searchSettings.getMinSegmentRequest();
			this.segmentTolerance = searchSettings.getSegmentTolerance();

			this.segmentQueryCacheSize = searchSettings.getSegmentQueryCacheSize();
			this.segmentQueryCacheMaxAmount = searchSettings.getSegmentQueryCacheMaxAmount();
		}
		else {
			defaultSearchField = null;
			requestFactor = null;
			segmentTolerance = null;
			segmentQueryCacheSize = null;
			segmentQueryCacheMaxAmount = null;
		}

		if (indexSettings.hasCommitSettings()) {
			CommitSettings commitSettings = indexSettings.getCommitSettings();
			this.segmentCommitInterval = commitSettings.getSegmentCommitInterval();
			this.idleTimeWithoutCommit = commitSettings.getIdleTimeWithoutCommit();
		}
		else {

		}

		this.fieldMap = new TreeMap<>();

		for (FieldConfig fc : indexSettings.getFieldConfigList()) {
			fieldMap.put(fc.getStoredFieldName(), fc);
		}

	}

	public static IndexConfig fromIndexSettings(IndexSettings indexSettings) {
		IndexConfig ic = new IndexConfig();
		ic.configure(indexSettings);
		return ic;
	}

}

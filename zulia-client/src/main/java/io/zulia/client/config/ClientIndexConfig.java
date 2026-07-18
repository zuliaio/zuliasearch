package io.zulia.client.config;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.zulia.client.command.builder.FieldMappingBuilder;
import io.zulia.client.command.builder.Search;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import static io.zulia.message.ZuliaBase.Similarity;
import static io.zulia.message.ZuliaIndex.AnalyzerSettings;
import static io.zulia.message.ZuliaIndex.FieldConfig;
import static io.zulia.message.ZuliaIndex.IndexSettings;

public class ClientIndexConfig {

	private List<String> defaultSearchFields = new ArrayList<>();
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
	private Integer defaultConcurrency;
	private String description;
	private Integer maxMergeThreads;
	private Integer maxMergePending;
	private Integer indexingThrottle;
	private Integer nrtIndexMaxMergeSizeMB;
	private Integer nrtIndexMaxCachedMB;
	private Integer nrtTaxoMaxMergeSizeMB;
	private Integer nrtTaxoMaxCachedMB;
	private Boolean nrtCachingDisabled;
	private Boolean transientIndex;

	private Boolean disableCompression;

	private TreeMap<String, FieldConfig> fieldMap;
	private TreeMap<String, AnalyzerSettings> analyzerSettingsMap;

	private TreeMap<String, ZuliaIndex.FieldMapping> fieldMappingMap;

	private Document meta;

	private List<QueryRequest> warmingSearches;

	public ClientIndexConfig() {
		this.fieldMap = new TreeMap<>();
		this.analyzerSettingsMap = new TreeMap<>();
		this.fieldMappingMap = new TreeMap<>();
		this.warmingSearches = new ArrayList<>();

	}

	public ClientIndexConfig addDefaultSearchField(String defaultSearchField) {
		defaultSearchFields.add(defaultSearchField);
		return this;
	}

	public ClientIndexConfig addDefaultSearchFields(String... defaultSearchField) {
		defaultSearchFields.addAll(Arrays.asList(defaultSearchField));
		return this;
	}

	public List<String> getDefaultSearchFields() {
		return defaultSearchFields;
	}

	public ClientIndexConfig setDefaultSearchFields(List<String> defaultSearchFields) {
		// copy at every assignment so the field is always mutable for the add methods
		this.defaultSearchFields = new ArrayList<>(defaultSearchFields);
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

	public ClientIndexConfig setNumberOfShards(Integer numberOfShards) {
		this.numberOfShards = numberOfShards;
		return this;
	}

	public Integer getRamBufferMB() {
		return ramBufferMB;
	}

	public ClientIndexConfig setRamBufferMB(Integer ramBufferMB) {
		this.ramBufferMB = ramBufferMB;
		return this;
	}

	public Integer getMaxMergeThreads() {
		return maxMergeThreads;
	}

	public ClientIndexConfig setMaxMergeThreads(Integer maxMergeThreads) {
		this.maxMergeThreads = maxMergeThreads;
		return this;
	}

	public Integer getMaxMergePending() {
		return maxMergePending;
	}

	public ClientIndexConfig setMaxMergePending(Integer maxMergePending) {
		this.maxMergePending = maxMergePending;
		return this;
	}

	public Integer getIndexingThrottle() {
		return indexingThrottle;
	}

	public ClientIndexConfig setIndexingThrottle(Integer indexingThrottle) {
		this.indexingThrottle = indexingThrottle;
		return this;
	}

	public Integer getNrtIndexMaxMergeSizeMB() {
		return nrtIndexMaxMergeSizeMB;
	}

	public ClientIndexConfig setNrtIndexMaxMergeSizeMB(Integer nrtIndexMaxMergeSizeMB) {
		this.nrtIndexMaxMergeSizeMB = nrtIndexMaxMergeSizeMB;
		return this;
	}

	public Integer getNrtIndexMaxCachedMB() {
		return nrtIndexMaxCachedMB;
	}

	public ClientIndexConfig setNrtIndexMaxCachedMB(Integer nrtIndexMaxCachedMB) {
		this.nrtIndexMaxCachedMB = nrtIndexMaxCachedMB;
		return this;
	}

	public Integer getNrtTaxoMaxMergeSizeMB() {
		return nrtTaxoMaxMergeSizeMB;
	}

	public ClientIndexConfig setNrtTaxoMaxMergeSizeMB(Integer nrtTaxoMaxMergeSizeMB) {
		this.nrtTaxoMaxMergeSizeMB = nrtTaxoMaxMergeSizeMB;
		return this;
	}

	public Integer getNrtTaxoMaxCachedMB() {
		return nrtTaxoMaxCachedMB;
	}

	public ClientIndexConfig setNrtTaxoMaxCachedMB(Integer nrtTaxoMaxCachedMB) {
		this.nrtTaxoMaxCachedMB = nrtTaxoMaxCachedMB;
		return this;
	}

	public Boolean getNrtCachingDisabled() {
		return nrtCachingDisabled;
	}

	public ClientIndexConfig setNrtCachingDisabled(Boolean nrtCachingDisabled) {
		this.nrtCachingDisabled = nrtCachingDisabled;
		return this;
	}

	public Boolean getTransientIndex() {
		return transientIndex;
	}

	/**
	 * When true, the index loads lazily and may be unloaded from memory after idle time or under the node's
	 * transient cache bound, then reloaded on demand. Default false: the index loads at startup and stays resident.
	 */
	public ClientIndexConfig setTransientIndex(Boolean transientIndex) {
		this.transientIndex = transientIndex;
		return this;
	}

	public Boolean getDisableCompression() {
		return disableCompression;
	}

	public ClientIndexConfig setDisableCompression(Boolean disableCompression) {
		this.disableCompression = disableCompression;
		return this;
	}

	public Integer getDefaultConcurrency() {
		return defaultConcurrency;
	}

	public ClientIndexConfig setDefaultConcurrency(Integer defaultConcurrency) {
		this.defaultConcurrency = defaultConcurrency;
		return this;
	}

	public String getDescription() {
		return description;
	}

	public ClientIndexConfig setDescription(String description) {
		this.description = description;
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

	public ClientIndexConfig setShardQueryCacheSize(Integer shardQueryCacheSize) {
		this.shardQueryCacheSize = shardQueryCacheSize;
		return this;
	}

	public Integer getShardQueryCacheMaxAmount() {
		return shardQueryCacheMaxAmount;
	}

	public ClientIndexConfig setShardQueryCacheMaxAmount(Integer shardQueryCacheMaxAmount) {
		this.shardQueryCacheMaxAmount = shardQueryCacheMaxAmount;
		return this;
	}

	public ClientIndexConfig addFieldConfig(FieldConfigBuilder FieldConfigBuilder) {
		addFieldConfig(FieldConfigBuilder.build());
		return this;
	}

	public ClientIndexConfig addFieldConfig(FieldConfig fieldConfig) {
		this.fieldMap.put(fieldConfig.getStoredFieldName(), fieldConfig);
		return this;
	}

	public FieldConfig getFieldConfig(String fieldName) {
		return this.fieldMap.get(fieldName);
	}

	public Integer getIndexWeight() {
		return indexWeight;
	}

	public ClientIndexConfig setIndexWeight(Integer indexWeight) {
		this.indexWeight = indexWeight;
		return this;
	}

	public Integer getNumberOfReplicas() {
		return numberOfReplicas;
	}

	public ClientIndexConfig setNumberOfReplicas(Integer numberOfReplicas) {
		this.numberOfReplicas = numberOfReplicas;
		return this;
	}

	public List<QueryRequest> getWarmingSearches() {
		return warmingSearches;
	}

	public ClientIndexConfig setWarmingSearches(List<QueryRequest> warmingSearches) {
		// copy at every assignment so the field is always mutable for addWarmingSearch
		this.warmingSearches = warmingSearches == null ? new ArrayList<>() : new ArrayList<>(warmingSearches);
		return this;
	}

	public ClientIndexConfig addAnalyzerSetting(String name, AnalyzerSettings.Tokenizer tokenizer, Iterable<AnalyzerSettings.Filter> filterList,
			Similarity similarity) {

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

		return addAnalyzerSetting(analyzerSettings.build());
	}

	public ClientIndexConfig addAnalyzerSetting(AnalyzerSettings analyzerSettings) {
		analyzerSettingsMap.put(analyzerSettings.getName(), analyzerSettings);
		return this;
	}

	public AnalyzerSettings getAnalyzerSettings(String analyzerName) {
		return analyzerSettingsMap.get(analyzerName);
	}

	public TreeMap<String, AnalyzerSettings> getAnalyzerSettingsMap() {
		return analyzerSettingsMap;
	}

	public TreeMap<String, FieldConfig> getFieldConfigMap() {
		return fieldMap;
	}

	public ClientIndexConfig setMeta(Document meta) {
		this.meta = meta;
		return this;
	}

	public Document getMeta() {
		return meta;
	}

	public ClientIndexConfig addFieldMapping(String alias, Iterable<String> fieldOrFieldPattern, boolean includeSelf) {
		ZuliaIndex.FieldMapping.Builder fieldMapping = ZuliaIndex.FieldMapping.newBuilder();
		fieldMapping.setAlias(alias);
		fieldMapping.addAllFieldOrFieldPattern(fieldOrFieldPattern);
		fieldMapping.setIncludeSelf(includeSelf);
		return addFieldMapping(fieldMapping.build());
	}

	public ClientIndexConfig addFieldMapping(FieldMappingBuilder fieldMappingBuilder) {
		return addFieldMapping(fieldMappingBuilder.getFieldMapping());
	}

	@NotNull
	private ClientIndexConfig addFieldMapping(ZuliaIndex.FieldMapping fieldMapping) {
		fieldMappingMap.put(fieldMapping.getAlias(), fieldMapping);
		return this;
	}

	public TreeMap<String, ZuliaIndex.FieldMapping> getFieldMappingMap() {
		return fieldMappingMap;
	}

	public IndexSettings getIndexSettings() {
		IndexSettings.Builder isb = IndexSettings.newBuilder();

		if (indexName != null) {
			isb.setIndexName(indexName);
		}

		if (numberOfShards != null) {
			isb.setNumberOfShards(numberOfShards);
		}

		if (numberOfReplicas != null) {
			isb.setNumberOfReplicas(numberOfReplicas);
		}

		if (defaultSearchFields != null) {
			isb.addAllDefaultSearchField(defaultSearchFields);
		}

		for (String analyzerName : analyzerSettingsMap.keySet()) {
			isb.addAnalyzerSettings(analyzerSettingsMap.get(analyzerName));
		}

		for (String fieldName : fieldMap.keySet()) {
			FieldConfig fieldConfig = fieldMap.get(fieldName);
			isb.addFieldConfig(fieldConfig);
		}

		if (requestFactor != null) {
			isb.setRequestFactor(requestFactor);
		}

		if (minShardRequest != null) {
			isb.setMinShardRequest(minShardRequest);
		}

		if (shardTolerance != null) {
			isb.setShardTolerance(shardTolerance);
		}

		if (shardQueryCacheSize != null) {
			isb.setShardQueryCacheSize(shardQueryCacheSize);
		}

		if (shardQueryCacheMaxAmount != null) {
			isb.setShardQueryCacheMaxAmount(shardQueryCacheMaxAmount);
		}

		if (idleTimeWithoutCommit != null) {
			isb.setIdleTimeWithoutCommit(idleTimeWithoutCommit);
		}

		if (shardCommitInterval != null) {
			isb.setShardCommitInterval(shardCommitInterval);
		}

		if (indexWeight != null) {
			isb.setIndexWeight(indexWeight);
		}

		if (ramBufferMB != null) {
			isb.setRamBufferMB(ramBufferMB);
		}

		if (maxMergeThreads != null) {
			isb.setMaxMergeThreads(maxMergeThreads);
		}

		if (maxMergePending != null) {
			isb.setMaxMergePending(maxMergePending);
		}

		if (indexingThrottle != null) {
			isb.setIndexingThrottle(indexingThrottle);
		}

		if (nrtIndexMaxMergeSizeMB != null) {
			isb.setNrtIndexMaxMergeSizeMB(nrtIndexMaxMergeSizeMB);
		}

		if (nrtIndexMaxCachedMB != null) {
			isb.setNrtIndexMaxCachedMB(nrtIndexMaxCachedMB);
		}

		if (nrtTaxoMaxMergeSizeMB != null) {
			isb.setNrtTaxoMaxMergeSizeMB(nrtTaxoMaxMergeSizeMB);
		}

		if (nrtTaxoMaxCachedMB != null) {
			isb.setNrtTaxoMaxCachedMB(nrtTaxoMaxCachedMB);
		}

		if (nrtCachingDisabled != null) {
			isb.setNrtCachingDisabled(nrtCachingDisabled);
		}

		if (transientIndex != null) {
			isb.setTransientIndex(transientIndex);
		}

		if (disableCompression != null) {
			isb.setDisableCompression(disableCompression);
		}

		if (defaultConcurrency != null) {
			isb.setDefaultConcurrency(defaultConcurrency);
		}

		if (description != null) {
			isb.setDescription(description);
		}

		if (meta != null) {
			isb.setMeta(ZuliaUtil.mongoDocumentToByteString(meta));
		}

		if (warmingSearches != null) {
			for (QueryRequest queryRequest : warmingSearches) {
				isb.addWarmingSearches(queryRequest.toByteString());
			}

		}

		if (fieldMappingMap != null) {
			for (String alias : fieldMappingMap.keySet()) {
				ZuliaIndex.FieldMapping fieldMapping = fieldMappingMap.get(alias);
				isb.addFieldMapping(fieldMapping);
			}
		}

		return isb.build();
	}

	public ClientIndexConfig addWarmingSearch(Search search) {
		this.warmingSearches.add(search.getRequest());
		return this;
	}

	public ClientIndexConfig addWarmingSearch(QueryRequest queryRequest) {
		this.warmingSearches.add(queryRequest);
		return this;
	}

	public void configure(IndexSettings indexSettings) {
		this.indexName = indexSettings.getIndexName();
		this.numberOfShards = indexSettings.getNumberOfShards();
		this.numberOfReplicas = indexSettings.getNumberOfReplicas();
		this.defaultSearchFields = new ArrayList<>(indexSettings.getDefaultSearchFieldList());

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
		this.maxMergeThreads = indexSettings.getMaxMergeThreads();
		this.maxMergePending = indexSettings.getMaxMergePending();
		this.indexingThrottle = indexSettings.getIndexingThrottle();
		this.nrtIndexMaxMergeSizeMB = indexSettings.getNrtIndexMaxMergeSizeMB();
		this.nrtIndexMaxCachedMB = indexSettings.getNrtIndexMaxCachedMB();
		this.nrtTaxoMaxMergeSizeMB = indexSettings.getNrtTaxoMaxMergeSizeMB();
		this.nrtTaxoMaxCachedMB = indexSettings.getNrtTaxoMaxCachedMB();
		this.nrtCachingDisabled = indexSettings.getNrtCachingDisabled();
		this.transientIndex = indexSettings.getTransientIndex();
		this.disableCompression = indexSettings.getDisableCompression();
		this.defaultConcurrency = indexSettings.getDefaultConcurrency();
		this.description = indexSettings.getDescription();

		this.meta = ZuliaUtil.byteStringToMongoDocument(indexSettings.getMeta());

		this.warmingSearches = new ArrayList<>();
		for (ByteString byteString : indexSettings.getWarmingSearchesList()) {
			try {
				QueryRequest queryRequest = QueryRequest.parseFrom(byteString);
				this.warmingSearches.add(queryRequest);
			}
			catch (InvalidProtocolBufferException e) {
				throw new RuntimeException(e);
			}
		}

		this.fieldMappingMap = new TreeMap<>();
		for (ZuliaIndex.FieldMapping fieldMapping : indexSettings.getFieldMappingList()) {
			fieldMappingMap.put(fieldMapping.getAlias(), fieldMapping);
		}

	}

	public static ClientIndexConfig fromIndexSettings(IndexSettings indexSettings) {
		ClientIndexConfig ic = new ClientIndexConfig();
		ic.configure(indexSettings);
		return ic;
	}

}

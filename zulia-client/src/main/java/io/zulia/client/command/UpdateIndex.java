package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.command.base.SingleIndexRoutableCommand;
import io.zulia.client.command.builder.FieldMappingBuilder;
import io.zulia.client.command.builder.Search;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.UpdateIndexResult;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldMapping;
import io.zulia.message.ZuliaIndex.UpdateIndexSettings;
import io.zulia.message.ZuliaIndex.UpdateIndexSettings.Operation.OperationType;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.message.ZuliaServiceOuterClass.UpdateIndexRequest;
import io.zulia.message.ZuliaServiceOuterClass.UpdateIndexResponse;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;

/**
 * Allows partial index changes.  To replace the entire index config use {@link CreateIndex}
 *
 * @author mdavis
 */
public class UpdateIndex extends SimpleCommand<UpdateIndexRequest, UpdateIndexResult> implements SingleIndexRoutableCommand {

	private Double requestFactor;
	private Integer minShardRequest;

	private String indexName;
	private Integer idleTimeWithoutCommit;
	private Integer shardCommitInterval;
	private Double shardTolerance;
	private Integer shardQueryCacheSize;
	private Integer shardQueryCacheMaxAmount;
	private Integer indexWeight;
	private Integer ramBufferMB;
	private Integer numberOfReplicas;

	private Boolean disableCompression;

	private final UpdateIndexSettings.Operation.Builder analyzerSettingsOperation = UpdateIndexSettings.Operation.newBuilder();
	private List<ZuliaIndex.AnalyzerSettings> analyzerSettingsList = Collections.emptyList();

	private final UpdateIndexSettings.Operation.Builder fieldConfigOperation = UpdateIndexSettings.Operation.newBuilder();
	private List<ZuliaIndex.FieldConfig> fieldConfigList = Collections.emptyList();
	private UpdateIndexSettings.Operation.Builder metaDataOperation = UpdateIndexSettings.Operation.newBuilder();
	private Document metadata = new Document();

	private UpdateIndexSettings.Operation.Builder warmingSearchOperation = UpdateIndexSettings.Operation.newBuilder();

	private List<QueryRequest> warmingSearches = Collections.emptyList();

	private final UpdateIndexSettings.Operation.Builder fieldMappingOperation = UpdateIndexSettings.Operation.newBuilder();
	private List<ZuliaIndex.FieldMapping> fieldMappingList = Collections.emptyList();

	public UpdateIndex(String indexName) {
		this.indexName = indexName;
	}

	public UpdateIndex clearAnalyzerSettingsChanges() {
		this.analyzerSettingsOperation.setEnable(false);
		this.analyzerSettingsOperation.setOperationType(OperationType.MERGE);
		this.analyzerSettingsOperation.clearRemovedKeys();
		if (!this.analyzerSettingsList.isEmpty()) {
			this.analyzerSettingsList.clear();
		}
		return this;
	}

	public UpdateIndex removeAnalyzerSettingsByName(String... namesToRemove) {
		return removeAnalyzerSettingsByName(Arrays.asList(namesToRemove));
	}

	public UpdateIndex removeAnalyzerSettingsByName(Collection<String> namesToRemove) {
		this.analyzerSettingsOperation.setEnable(true);
		this.analyzerSettingsOperation.addAllRemovedKeys(namesToRemove);
		return this;
	}

	public UpdateIndex mergeAnalyzerSettings(ZuliaIndex.AnalyzerSettings... analyzerSettings) {
		return mergeAnalyzerSettings(List.of(analyzerSettings));
	}

	public UpdateIndex mergeAnalyzerSettings(List<ZuliaIndex.AnalyzerSettings> analyzerSettings) {
		if (analyzerSettings == null || analyzerSettings.isEmpty()) {
			throw new IllegalArgumentException("Cannot merge null or empty analyzer settings");
		}
		this.analyzerSettingsOperation.setEnable(true);
		this.analyzerSettingsOperation.setOperationType(OperationType.MERGE);
		this.analyzerSettingsList = analyzerSettings;
		return this;
	}

	public UpdateIndex replaceAnalyzerSettings(ZuliaIndex.AnalyzerSettings... analyzerSettings) {
		return replaceAnalyzerSettings(Arrays.asList(analyzerSettings));
	}

	public UpdateIndex replaceAnalyzerSettings(List<ZuliaIndex.AnalyzerSettings> analyzerSettings) {

		if (analyzerSettings == null) {
			analyzerSettings = Collections.emptyList();
		}

		this.analyzerSettingsOperation.setEnable(true);
		this.analyzerSettingsOperation.setOperationType(OperationType.REPLACE);
		this.analyzerSettingsList = analyzerSettings;
		return this;
	}

	public UpdateIndex clearFieldConfigChanges() {
		this.fieldConfigOperation.setEnable(false);
		this.fieldConfigOperation.setOperationType(OperationType.MERGE);
		this.fieldConfigOperation.clearRemovedKeys();
		if (!this.fieldConfigList.isEmpty()) {
			this.fieldConfigList.clear();
		}
		return this;
	}

	public UpdateIndex removeFieldConfigByStoredName(String... namesToRemove) {
		return removeFieldConfigByStoredName(Arrays.asList(namesToRemove));
	}

	public UpdateIndex removeFieldConfigByStoredName(Collection<String> namesToRemove) {
		this.fieldConfigOperation.setEnable(true);
		this.fieldConfigOperation.addAllRemovedKeys(namesToRemove);
		return this;
	}

	public UpdateIndex replaceFieldConfig(ZuliaIndex.FieldConfig... fieldConfigs) {
		return replaceFieldConfig(Arrays.asList(fieldConfigs));
	}

	public UpdateIndex replaceFieldConfig(FieldConfigBuilder... fieldConfigs) {
		return replaceFieldConfig(Arrays.stream(fieldConfigs).map(FieldConfigBuilder::build).collect(Collectors.toList()));
	}

	public UpdateIndex replaceFieldConfig(List<ZuliaIndex.FieldConfig> fieldConfigs) {
		if (fieldConfigs == null) {
			fieldConfigs = Collections.emptyList();
		}

		this.fieldConfigOperation.setEnable(true);
		this.fieldConfigOperation.setOperationType(OperationType.REPLACE);
		this.fieldConfigList = fieldConfigs;
		return this;
	}

	public UpdateIndex mergeFieldConfig(ZuliaIndex.FieldConfig... fieldConfigs) {
		return mergeFieldConfig(Arrays.asList(fieldConfigs));
	}

	public UpdateIndex mergeFieldConfig(FieldConfigBuilder... fieldConfigs) {
		return mergeFieldConfig(Arrays.stream(fieldConfigs).map(FieldConfigBuilder::build).toList());
	}

	public UpdateIndex mergeFieldConfig(List<ZuliaIndex.FieldConfig> fieldConfigs) {
		if (fieldConfigs == null || fieldConfigs.isEmpty()) {
			throw new IllegalArgumentException("Cannot merge null or empty warming searches");
		}

		this.fieldConfigOperation.setEnable(true);
		this.fieldConfigOperation.setOperationType(OperationType.MERGE);
		this.fieldConfigList = fieldConfigs;
		return this;
	}

	public UpdateIndex removeFieldMappingByAlias(String... aliasesToRemove) {
		return removeFieldMappingByAlias(Arrays.asList(aliasesToRemove));
	}

	public UpdateIndex removeFieldMappingByAlias(Collection<String> aliasesToRemove) {
		this.fieldMappingOperation.setEnable(true);
		this.fieldMappingOperation.addAllRemovedKeys(aliasesToRemove);
		return this;
	}

	public UpdateIndex replaceFieldMapping(FieldMapping... fieldMapping) {
		return replaceFieldMapping(Arrays.asList(fieldMapping));
	}

	public UpdateIndex replaceFieldMapping(FieldMappingBuilder... fieldMapping) {
		return replaceFieldMapping(Arrays.stream(fieldMapping).map(FieldMappingBuilder::getFieldMapping).collect(Collectors.toList()));
	}

	public UpdateIndex replaceFieldMapping(List<FieldMapping> fieldMappings) {
		if (fieldMappings == null) {
			fieldMappings = Collections.emptyList();
		}

		this.fieldMappingOperation.setEnable(true);
		this.fieldMappingOperation.setOperationType(OperationType.REPLACE);
		this.fieldMappingList = fieldMappings;
		return this;
	}

	public UpdateIndex mergeFieldMapping(FieldMapping... fieldMappings) {
		return mergeFieldMapping(Arrays.asList(fieldMappings));
	}

	public UpdateIndex mergeFieldMapping(FieldMappingBuilder... mergeFieldMappings) {
		return mergeFieldMapping(Arrays.stream(mergeFieldMappings).map(FieldMappingBuilder::getFieldMapping).toList());
	}

	public UpdateIndex mergeFieldMapping(List<FieldMapping> fieldMappings) {
		if (fieldMappings == null || fieldMappings.isEmpty()) {
			throw new IllegalArgumentException("Cannot merge null or empty field mappings");
		}

		this.fieldMappingOperation.setEnable(true);
		this.fieldMappingOperation.setOperationType(OperationType.MERGE);
		this.fieldMappingList = fieldMappings;
		return this;
	}

	public Double getRequestFactor() {
		return requestFactor;
	}

	public UpdateIndex setRequestFactor(Double requestFactor) {
		this.requestFactor = requestFactor;
		return this;
	}

	public Integer getMinShardRequest() {
		return minShardRequest;
	}

	public UpdateIndex setMinShardRequest(Integer minShardRequest) {
		this.minShardRequest = minShardRequest;
		return this;
	}

	public String getIndexName() {
		return indexName;
	}

	public UpdateIndex setIndexName(String indexName) {
		this.indexName = indexName;
		return this;
	}

	public Integer getIdleTimeWithoutCommit() {
		return idleTimeWithoutCommit;
	}

	public UpdateIndex setIdleTimeWithoutCommit(Integer idleTimeWithoutCommit) {
		this.idleTimeWithoutCommit = idleTimeWithoutCommit;
		return this;
	}

	public Integer getShardCommitInterval() {
		return shardCommitInterval;
	}

	public UpdateIndex setShardCommitInterval(Integer shardCommitInterval) {
		this.shardCommitInterval = shardCommitInterval;
		return this;
	}

	public Double getShardTolerance() {
		return shardTolerance;
	}

	public UpdateIndex setShardTolerance(Double shardTolerance) {
		this.shardTolerance = shardTolerance;
		return this;
	}

	public Integer getShardQueryCacheSize() {
		return shardQueryCacheSize;
	}

	public UpdateIndex setShardQueryCacheSize(Integer shardQueryCacheSize) {
		this.shardQueryCacheSize = shardQueryCacheSize;
		return this;
	}

	public Integer getShardQueryCacheMaxAmount() {
		return shardQueryCacheMaxAmount;
	}

	public UpdateIndex setShardQueryCacheMaxAmount(Integer shardQueryCacheMaxAmount) {
		this.shardQueryCacheMaxAmount = shardQueryCacheMaxAmount;
		return this;
	}

	public Integer getIndexWeight() {
		return indexWeight;
	}

	public UpdateIndex setIndexWeight(Integer indexWeight) {
		this.indexWeight = indexWeight;
		return this;
	}

	public Integer getRamBufferMB() {
		return ramBufferMB;
	}

	public UpdateIndex setRamBufferMB(Integer ramBufferMB) {
		this.ramBufferMB = ramBufferMB;
		return this;
	}

	public Boolean getDisableCompression() {
		return disableCompression;
	}

	public UpdateIndex setDisableCompression(Boolean disableCompression) {
		this.disableCompression = disableCompression;
		return this;
	}

	public Integer getNumberOfReplicas() {
		return numberOfReplicas;
	}

	public UpdateIndex setNumberOfReplicas(Integer numberOfReplicas) {
		this.numberOfReplicas = numberOfReplicas;
		return this;
	}

	public UpdateIndex clearMetadataChanges() {
		this.metaDataOperation.setEnable(false);
		this.metaDataOperation.setOperationType(OperationType.MERGE);
		this.metaDataOperation.clearRemovedKeys();
		metadata = new Document();
		return this;
	}

	public UpdateIndex removeMetadataByKey(String... keysToRemove) {
		return removeMetadataByKey(Arrays.asList(keysToRemove));
	}

	public UpdateIndex removeMetadataByKey(Collection<String> keysToRemove) {
		this.metaDataOperation.setEnable(true);
		this.metaDataOperation.addAllRemovedKeys(keysToRemove);
		return this;
	}

	public UpdateIndex replaceMetadata(Document metadata) {
		this.metaDataOperation.setEnable(true);
		this.metaDataOperation.setOperationType(OperationType.REPLACE);
		this.metadata = Objects.requireNonNullElse(metadata, new Document());
		return this;
	}

	public UpdateIndex mergeMetadata(Document metadata) {
		if (metadata == null || metadata.isEmpty()) {
			throw new IllegalArgumentException("Cannot merge null or empty metadata");
		}

		this.metaDataOperation.setEnable(true);
		this.metaDataOperation.setOperationType(OperationType.MERGE);
		this.metadata = Objects.requireNonNullElse(metadata, new Document());
		return this;
	}

	public UpdateIndex clearWarmingSearchChanges() {
		warmingSearchOperation.setEnable(false);
		warmingSearchOperation.setOperationType(OperationType.MERGE);
		warmingSearchOperation.clearRemovedKeys();
		if (!warmingSearches.isEmpty()) {
			warmingSearches.clear();
		}
		return this;
	}

	public UpdateIndex removeWarmingSearchesByLabel(String... namesToRemove) {
		return removeWarmingSearchesByLabel(Arrays.asList(namesToRemove));
	}

	public UpdateIndex removeWarmingSearchesByLabel(Collection<String> namesToRemove) {
		warmingSearchOperation.setEnable(true);
		warmingSearchOperation.addAllRemovedKeys(namesToRemove);
		return this;
	}

	public UpdateIndex mergeWarmingSearches(Search... searches) {
		return mergeWarmingSearches(Arrays.asList(searches));
	}

	public UpdateIndex mergeWarmingSearches(Collection<Search> searches) {
		List<QueryRequest> queryRequests = searches.stream().map(Search::getRequest).toList();
		return mergeWarmingSearches(queryRequests);
	}

	private UpdateIndex mergeWarmingSearches(List<QueryRequest> queryRequests) {
		if (queryRequests == null || queryRequests.isEmpty()) {
			throw new IllegalArgumentException("Cannot merge null or empty warming searches");
		}
		this.warmingSearchOperation.setEnable(true);
		this.warmingSearchOperation.setOperationType(OperationType.MERGE);
		this.warmingSearches = queryRequests;
		return this;
	}

	public UpdateIndex replaceWarmingSearches(Search... searches) {
		return replaceWarmingSearches(Arrays.asList(searches));
	}

	public UpdateIndex replaceWarmingSearches(Collection<Search> searches) {
		return replaceWarmingSearches(searches.stream().map(Search::getRequest).toList());
	}

	public UpdateIndex replaceWarmingSearches(List<QueryRequest> queryRequests) {
		if (queryRequests == null) {
			queryRequests = Collections.emptyList();
		}
		this.warmingSearchOperation.setEnable(true);
		this.warmingSearchOperation.setOperationType(OperationType.REPLACE);
		this.warmingSearches = queryRequests;
		return this;
	}

	@Override
	public UpdateIndexRequest getRequest() {
		UpdateIndexRequest.Builder updateIndexRequestBuilder = UpdateIndexRequest.newBuilder();

		if (indexName == null) {
			throw new IllegalArgumentException("Index name is required");
		}

		updateIndexRequestBuilder.setIndexName(indexName);

		UpdateIndexSettings.Builder updateIndexSettings = UpdateIndexSettings.newBuilder();

		updateIndexSettings.addAllAnalyzerSettings(analyzerSettingsList);
		updateIndexSettings.setAnalyzerSettingsOperation(analyzerSettingsOperation);

		updateIndexSettings.addAllFieldConfig(fieldConfigList);
		updateIndexSettings.setFieldConfigOperation(fieldConfigOperation);

		updateIndexSettings.addAllFieldMapping(fieldMappingList);
		updateIndexSettings.setFieldMappingOperation(fieldMappingOperation);

		updateIndexSettings.addAllWarmingSearches(warmingSearches.stream().map(QueryRequest::toByteString).toList());
		updateIndexSettings.setWarmingSearchesOperation(warmingSearchOperation);

		if (requestFactor != null) {
			updateIndexSettings.setSetRequestFactor(true);
			updateIndexSettings.setRequestFactor(requestFactor);
		}

		if (minShardRequest != null) {
			updateIndexSettings.setSetMinShardRequest(true);
			updateIndexSettings.setMinShardRequest(minShardRequest);
		}

		if (shardTolerance != null) {
			updateIndexSettings.setSetShardTolerance(true);
			updateIndexSettings.setShardTolerance(shardTolerance);
		}

		if (shardQueryCacheSize != null) {
			updateIndexSettings.setSetShardQueryCacheSize(true);
			updateIndexSettings.setShardQueryCacheSize(shardQueryCacheSize);
		}

		if (shardQueryCacheMaxAmount != null) {
			updateIndexSettings.setSetShardQueryCacheMaxAmount(true);
			updateIndexSettings.setShardQueryCacheMaxAmount(shardQueryCacheMaxAmount);
		}

		if (idleTimeWithoutCommit != null) {
			updateIndexSettings.setSetIdleTimeWithoutCommit(true);
			updateIndexSettings.setIdleTimeWithoutCommit(idleTimeWithoutCommit);
		}

		if (shardCommitInterval != null) {
			updateIndexSettings.setSetShardCommitInterval(true);
			updateIndexSettings.setShardCommitInterval(shardCommitInterval);
		}

		if (indexWeight != null) {
			updateIndexSettings.setSetIndexWeight(true);
			updateIndexSettings.setIndexWeight(indexWeight);
		}

		if (ramBufferMB != null) {
			updateIndexSettings.setSetRamBufferMB(true);
			updateIndexSettings.setRamBufferMB(ramBufferMB);
		}

		if (disableCompression != null) {
			updateIndexSettings.setSetDisableCompression(true);
			updateIndexSettings.setDisableCompression(disableCompression);
		}

		updateIndexSettings.setMetaUpdateOperation(metaDataOperation);
		if (!metadata.isEmpty()) {
			updateIndexSettings.setMetadata(ZuliaUtil.mongoDocumentToByteString(metadata));
		}

		updateIndexRequestBuilder.setUpdateIndexSettings(updateIndexSettings);
		return updateIndexRequestBuilder.build();
	}

	@Override
	public UpdateIndexResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();
		UpdateIndexResponse updateIndexResponse = service.updateIndex(getRequest());

		return new UpdateIndexResult(updateIndexResponse);
	}

}

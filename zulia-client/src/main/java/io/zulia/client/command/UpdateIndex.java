package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.command.base.SingleIndexRoutableCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.UpdateIndexResult;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.UpdateIndexSettings;
import io.zulia.message.ZuliaIndex.UpdateIndexSettings.Operation.OperationType;
import io.zulia.message.ZuliaServiceOuterClass.UpdateIndexRequest;
import io.zulia.message.ZuliaServiceOuterClass.UpdateIndexResponse;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;

/**
 * Allows partial index changes.  To replace the entire index config use {@link CreateIndex}
 * @author mdavis
 *
 */
public class UpdateIndex extends SimpleCommand<UpdateIndexRequest, UpdateIndexResult> implements SingleIndexRoutableCommand {

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

	private final UpdateIndexSettings.Operation.Builder analyzerSettingsOperation = UpdateIndexSettings.Operation.newBuilder();
	private List<ZuliaIndex.AnalyzerSettings> analyzerSettingsList = Collections.emptyList();


	private final UpdateIndexSettings.Operation.Builder fieldConfigOperation = UpdateIndexSettings.Operation.newBuilder();
	private List<ZuliaIndex.FieldConfig> fieldConfigList = Collections.emptyList();
	private UpdateIndexSettings.Operation.Builder metaDataOperation = UpdateIndexSettings.Operation.newBuilder();
	private Document metadata = new Document();

	public UpdateIndex(String indexName) {
		this.indexName = indexName;
	}

	public UpdateIndex clearAnalyzerSettingsChanges() {
		analyzerSettingsOperation.setEnable(false);
		fieldConfigOperation.setOperationType(OperationType.MERGE);
		analyzerSettingsOperation.clearRemovedKeys();
		if (!analyzerSettingsList.isEmpty()) {
			analyzerSettingsList.clear();
		}
		return this;
	}

	public UpdateIndex removeAnalyzerSettingsByName(Collection<String> namesToRemove) {
		analyzerSettingsOperation.setEnable(true);
		analyzerSettingsOperation.addAllRemovedKeys(namesToRemove);
		return this;
	}

	public UpdateIndex mergeAnalyzerSettings(ZuliaIndex.AnalyzerSettings... analyzerSettings) {
		analyzerSettingsOperation.setEnable(true);
		analyzerSettingsOperation.setOperationType(OperationType.MERGE);
		this.analyzerSettingsList = Objects.requireNonNullElse(Arrays.asList(analyzerSettings), Collections.emptyList());
		return this;
	}

	public UpdateIndex replaceAnalyzerSettings(ZuliaIndex.AnalyzerSettings... analyzerSettings) {
		analyzerSettingsOperation.setEnable(true);
		analyzerSettingsOperation.setOperationType(OperationType.REPLACE);
		this.analyzerSettingsList = Objects.requireNonNullElse(Arrays.asList(analyzerSettings), Collections.emptyList());
		return this;
	}


	public UpdateIndex clearFieldConfigChanges() {
		fieldConfigOperation.setEnable(false);
		fieldConfigOperation.setOperationType(OperationType.MERGE);
		fieldConfigOperation.clearRemovedKeys();
		if (!fieldConfigList.isEmpty()) {
			fieldConfigList.clear();
		}
		return this;
	}

	public UpdateIndex removeFieldConfigByStoredName(Collection<String> namesToRemove) {
		fieldConfigOperation.setEnable(true);
		fieldConfigOperation.addAllRemovedKeys(namesToRemove);
		return this;
	}

	public UpdateIndex replaceFieldConfig(FieldConfigBuilder... fieldConfigs) {
		fieldConfigOperation.setEnable(true);
		fieldConfigOperation.setOperationType(OperationType.REPLACE);
		this.fieldConfigList =  Objects.requireNonNullElse(Arrays.stream(fieldConfigs).map(FieldConfigBuilder::build).toList(), Collections.emptyList());
		return this;
	}

	public UpdateIndex replaceFieldConfig(ZuliaIndex.FieldConfig... fieldConfigs) {
		fieldConfigOperation.setEnable(true);
		fieldConfigOperation.setOperationType(OperationType.REPLACE);
		this.fieldConfigList = Objects.requireNonNullElse(Arrays.asList(fieldConfigs), Collections.emptyList());
		return this;
	}


	public UpdateIndex mergeFieldConfig(FieldConfigBuilder... fieldConfigs) {
		fieldConfigOperation.setEnable(true);
		fieldConfigOperation.setOperationType(OperationType.MERGE);
		this.fieldConfigList =  Objects.requireNonNullElse(Arrays.stream(fieldConfigs).map(FieldConfigBuilder::build).toList(), Collections.emptyList());
		return this;
	}

	public UpdateIndex mergeFieldConfig(ZuliaIndex.FieldConfig... fieldConfigs) {
		fieldConfigOperation.setEnable(true);
		fieldConfigOperation.setOperationType(OperationType.MERGE);
		this.fieldConfigList = Objects.requireNonNullElse(Arrays.asList(fieldConfigs), Collections.emptyList());
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

	public Integer getNumberOfShards() {
		return numberOfShards;
	}

	public UpdateIndex setNumberOfShards(Integer numberOfShards) {
		this.numberOfShards = numberOfShards;
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

	public Integer getNumberOfReplicas() {
		return numberOfReplicas;
	}

	public UpdateIndex setNumberOfReplicas(Integer numberOfReplicas) {
		this.numberOfReplicas = numberOfReplicas;
		return this;
	}


	public UpdateIndex clearMetadataChanges() {
		metaDataOperation.setEnable(false);
		metaDataOperation.setOperationType(OperationType.MERGE);
		metaDataOperation.clearRemovedKeys();
		metadata = new Document();
		return this;
	}

	public UpdateIndex removeMetadataByKey(Collection<String> namesToRemove) {
		metaDataOperation.setEnable(true);
		metaDataOperation.addAllRemovedKeys(namesToRemove);
		return this;
	}

	public UpdateIndex replaceMetadata(Document metadata) {
		metaDataOperation.setEnable(true);
		metaDataOperation.setOperationType(OperationType.REPLACE);
		this.metadata = Objects.requireNonNullElse(metadata, new Document());
		return this;
	}

	public UpdateIndex mergeMetadata(Document metadata) {
		metaDataOperation.setEnable(true);
		metaDataOperation.setOperationType(OperationType.MERGE);
		this.metadata = Objects.requireNonNullElse(metadata, new Document());
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

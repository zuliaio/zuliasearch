package io.zulia.server.connection.server.validation;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaServiceOuterClass.CreateIndexRequest;

import java.util.List;

public class CreateIndexRequestValidator implements DefaultValidator<CreateIndexRequest> {

	public CreateIndexRequest validateAndSetDefault(CreateIndexRequest request) {
		CreateIndexRequest.Builder requestBuilder = request.toBuilder();

		if (!requestBuilder.hasIndexSettings()) {
			throw new IllegalArgumentException("Index settings field is required for create index");
		}

		IndexSettings.Builder indexSettings = requestBuilder.getIndexSettingsBuilder();

		if (indexSettings.getIndexName().isEmpty()) {
			throw new IllegalArgumentException("Index name must be provided");
		}

		if (indexSettings.getIndexWeight() == 0) {
			indexSettings.setIndexWeight(1);
		}
		else if (indexSettings.getIndexWeight() < 0) {
			throw new IllegalArgumentException("Index weight must be positive");
		}

		if (indexSettings.getNumberOfShards() == 0) {
			indexSettings.setNumberOfShards(1);
		}
		if (indexSettings.getNumberOfReplicas() == 0) {
			//indexSettings.setNumberOfReplicas(0);
		}
		if (indexSettings.getRequestFactor() == 0) {
			indexSettings.setRequestFactor(2.0);
		}
		if (indexSettings.getMinShardRequest() == 0) {
			indexSettings.setMinShardRequest(2);
		}
		if (indexSettings.getIdleTimeWithoutCommit() == 0) {
			indexSettings.setIdleTimeWithoutCommit(5);
		}
		if (indexSettings.getShardTolerance() == 0) {
			//currently 0 is the default
		}
		if (indexSettings.getShardQueryCacheSize() == 0) {
			indexSettings.setShardQueryCacheSize(512);
		}
		if (indexSettings.getShardQueryCacheMaxAmount() == 0) {
			indexSettings.setShardQueryCacheMaxAmount(256);
		}
		if (indexSettings.getShardCommitInterval() == 0) {
			indexSettings.setShardCommitInterval(3200);
		}

		List<ZuliaIndex.FieldConfig.Builder> fieldConfigBuilderList = indexSettings.getFieldConfigBuilderList();
		for (ZuliaIndex.FieldConfig.Builder builder : fieldConfigBuilderList) {
			for (ZuliaIndex.IndexAs indexAs : builder.getIndexAsList()) {
				if (indexAs.getIndexFieldName().contains(",")) {
					throw new IllegalArgumentException("Index as field name can not contain a comma");
				}
			}

		}

		return requestBuilder.build();
	}
}

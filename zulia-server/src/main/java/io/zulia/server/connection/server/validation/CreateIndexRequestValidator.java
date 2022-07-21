package io.zulia.server.connection.server.validation;

import io.zulia.DefaultAnalyzers;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaServiceOuterClass.CreateIndexRequest;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.util.ZuliaUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CreateIndexRequestValidator implements DefaultValidator<CreateIndexRequest> {

	public CreateIndexRequest validateAndSetDefault(CreateIndexRequest request) {
		CreateIndexRequest.Builder requestBuilder = request.toBuilder();

		if (!request.hasIndexSettings()) {
			throw new IllegalArgumentException("Index settings field is required for create index");
		}

		IndexSettings.Builder indexSettings = requestBuilder.getIndexSettingsBuilder();
		validateIndexSettingsAndSetDefaults(indexSettings);
		return requestBuilder.build();
	}

	public static void validateIndexSettingsAndSetDefaults(IndexSettings.Builder indexSettings) {
		if (indexSettings.getIndexName().isEmpty()) {
			throw new IllegalArgumentException("Index name must be provided");
		}

		if (indexSettings.getNumberOfShards() < 0) {
			throw new IllegalArgumentException("Number of shards cannot be negative");
		}
		else if (indexSettings.getNumberOfShards() == 0) {
			indexSettings.setNumberOfShards(1);
		}

		if (indexSettings.getIndexWeight() < 0) {
			throw new IllegalArgumentException("Index weight must be positive");
		}
		else if (indexSettings.getIndexWeight() == 0) {
			indexSettings.setIndexWeight(1);
		}

		if (indexSettings.getRequestFactor() < 0) {
			throw new IllegalArgumentException("Request factor must be positive");
		}
		else if (indexSettings.getRequestFactor() == 0) {
			indexSettings.setRequestFactor(2.0);
		}

		if (indexSettings.getMinShardRequest() < 0) {
			throw new IllegalArgumentException("Min Shard Request must be positive");
		}
		else if (indexSettings.getMinShardRequest() == 0) {
			indexSettings.setMinShardRequest(2);
		}

		if (indexSettings.getIdleTimeWithoutCommit() < 0) {
			throw new IllegalArgumentException("Idle Time Without Commit must be positive");
		}
		else if (indexSettings.getIdleTimeWithoutCommit() == 0) {
			indexSettings.setIdleTimeWithoutCommit(5);
		}

		if (indexSettings.getShardTolerance() < 0) {
			throw new IllegalArgumentException("Shard Tolerance must be positive");
		}
		else if (indexSettings.getShardTolerance() == 0) {
			//currently 0 is the default
		}

		if (indexSettings.getShardQueryCacheSize() < 0) {
			throw new IllegalArgumentException("Shard Query Cache Size must be positive");
		}
		else if (indexSettings.getShardQueryCacheSize() == 0) {
			indexSettings.setShardQueryCacheSize(512);
		}

		if (indexSettings.getShardQueryCacheMaxAmount() < 0) {
			throw new IllegalArgumentException("Shard Query Cache Max Amount must be positive");
		}
		else if (indexSettings.getShardQueryCacheMaxAmount() == 0) {
			indexSettings.setShardQueryCacheMaxAmount(256);
		}

		if (indexSettings.getShardCommitInterval() < 0) {
			throw new IllegalArgumentException("Shard Commit Interval must be positive");
		}
		else if (indexSettings.getShardCommitInterval() == 0) {
			indexSettings.setShardCommitInterval(3200);
		}

		HashSet<String> storedFields = new HashSet<>();

		Set<String> analyzerNames = new HashSet<>(indexSettings.getAnalyzerSettingsList().stream().map(ZuliaIndex.AnalyzerSettings::getName).toList());
		analyzerNames.addAll(DefaultAnalyzers.ALL_ANALYZERS);

		List<ZuliaIndex.FieldConfig.Builder> fieldConfigBuilderList = indexSettings.getFieldConfigBuilderList();
		for (ZuliaIndex.FieldConfig.Builder builder : fieldConfigBuilderList) {
			if (storedFields.contains(builder.getStoredFieldName())) {
				throw new IllegalArgumentException("Stored field <" + builder.getStoredFieldName() + "> is duplicated in field config");
			}
			storedFields.add(builder.getStoredFieldName());

			for (ZuliaIndex.IndexAs indexAs : builder.getIndexAsList()) {
				if (indexAs.getIndexFieldName().contains(",")) {
					throw new IllegalArgumentException("Index as field name can not contain a comma.  Found in stored field <" + builder.getStoredFieldName() + "> indexed as <" + indexAs.getIndexFieldName() + ">");
				}
				if (FieldTypeUtil.isStringFieldType(builder.getFieldType()) && !analyzerNames.contains(indexAs.getAnalyzerName())) {
					if (indexAs.getAnalyzerName().isEmpty()) {
						throw new IllegalArgumentException(
								"Analyzer is not defined for string field <" + builder.getStoredFieldName() + "> indexed as <" + indexAs.getIndexFieldName() + ">");
					}
					else {
						throw new IllegalArgumentException(
								"Analyzer <" + indexAs.getAnalyzerName() + "> is not a default analyzer and is not given as a custom analyzer for field <" + builder.getStoredFieldName() + "> indexed as <" + indexAs.getIndexFieldName() + ">");
					}
				}
			}

		}
	}
}

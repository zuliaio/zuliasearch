package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.command.builder.BatchDeleteGroupBuilder;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.BatchDeleteResult;
import io.zulia.client.result.CompleteResult;
import io.zulia.client.result.SearchResult;
import io.zulia.message.ZuliaServiceOuterClass.DeleteResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import static io.zulia.message.ZuliaServiceOuterClass.BatchDeleteRequest;

public class BatchDelete extends SimpleCommand<BatchDeleteRequest, BatchDeleteResult> {

	private final List<Delete> deletes;
	private final List<BatchDeleteGroupBuilder> groupList;

	public BatchDelete() {
		deletes = new ArrayList<>();
		groupList = new ArrayList<>();
	}

	public BatchDelete addDelete(Delete delete) {
		deletes.add(delete);
		return this;
	}

	public BatchDelete addDeleteGroup(BatchDeleteGroupBuilder group) {
		groupList.add(group);
		return this;
	}

	public BatchDelete addDeleteDocumentsFromUniqueIds(Collection<String> uniqueIds, String indexName) {
		groupList.add(new BatchDeleteGroupBuilder(indexName, uniqueIds));
		return this;
	}

	public BatchDelete deleteDocumentFromQueryResult(SearchResult queryResult) {

		Map<String, List<String>> indexToUniqueIds = new LinkedHashMap<>();
		for (CompleteResult completeResult : queryResult.getCompleteResults()) {
			indexToUniqueIds.computeIfAbsent(completeResult.getIndexName(), k -> new ArrayList<>()).add(completeResult.getUniqueId());
		}
		for (Map.Entry<String, List<String>> entry : indexToUniqueIds.entrySet()) {
			groupList.add(new BatchDeleteGroupBuilder(entry.getKey(), entry.getValue()));
		}

		return this;
	}

	@Override
	public BatchDeleteRequest getRequest() {
		BatchDeleteRequest.Builder batchDeleteRequest = BatchDeleteRequest.newBuilder();

		for (Delete delete : deletes) {
			batchDeleteRequest.addRequest(delete.getRequest());
		}
		for (BatchDeleteGroupBuilder group : groupList) {
			batchDeleteRequest.addBatchDeleteGroup(group.build());
		}

		return batchDeleteRequest.build();
	}

	@Override
	public BatchDeleteResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		Iterator<DeleteResponse> batchDeleteResponse = service.batchDelete(getRequest());

		return new BatchDeleteResult(batchDeleteResponse);
	}

}

package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.BatchFetchResult;
import io.zulia.client.result.CompleteResult;
import io.zulia.client.result.SearchResult;
import io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import io.zulia.message.ZuliaServiceOuterClass.BatchFetchRequest;
import io.zulia.message.ZuliaServiceOuterClass.FetchResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static io.zulia.message.ZuliaQuery.FetchType;

/**
 * Fetches multiple documents in a single call
 * @author mdavis
 *
 */
public class BatchFetch extends SimpleCommand<BatchFetchRequest, BatchFetchResult> {

	private List<Fetch> fetchList;

	public BatchFetch() {
		this.fetchList = new ArrayList<>();
	}

	public BatchFetch addFetches(Collection<? extends Fetch> fetches) {
		this.fetchList.addAll(fetches);
		return this;
	}

	public BatchFetch addFetchDocumentsFromUniqueIds(Collection<String> uniqueIds, String indexName) {

		for (String uniqueId : uniqueIds) {
			Fetch f = new Fetch(uniqueId, indexName);
			f.setResultFetchType(FetchType.FULL);
			f.setAssociatedFetchType(FetchType.NONE);
			fetchList.add(f);
		}
		return this;
	}

	public BatchFetch addFetchDocumentsFromResults(SearchResult qr) {
		return addFetchDocumentsFromResults(qr.getCompleteResults());
	}

	public BatchFetch addFetchDocumentsFromResults(Collection<CompleteResult> completeResults) {

		for (CompleteResult completeResult : completeResults) {
			Fetch f = new Fetch(completeResult.getUniqueId(), completeResult.getIndexName());
			f.setResultFetchType(FetchType.FULL);
			f.setAssociatedFetchType(FetchType.NONE);
			fetchList.add(f);
		}
		return this;
	}

	@Override
	public BatchFetchRequest getRequest() {
		BatchFetchRequest.Builder batchFetchRequestBuilder = BatchFetchRequest.newBuilder();
		for (Fetch f : fetchList) {
			batchFetchRequestBuilder.addFetchRequest(f.getRequest());
		}
		return batchFetchRequestBuilder.build();
	}

	@Override
	public BatchFetchResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		Iterator<FetchResponse> batchFetchResponse = service.batchFetch(getRequest());

		return new BatchFetchResult(batchFetchResponse);
	}

}

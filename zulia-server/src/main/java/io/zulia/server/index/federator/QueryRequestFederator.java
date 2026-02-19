package io.zulia.server.index.federator;

import com.google.protobuf.util.JsonFormat;
import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaQuery.IndexShardResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalQueryRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalQueryResponse;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.message.ZuliaServiceOuterClass.QueryResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndex;
import io.zulia.server.search.QueryCombiner;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class QueryRequestFederator extends MasterSlaveNodeRequestFederator<QueryRequest, InternalQueryResponse> {

	private static final Logger LOG = LoggerFactory.getLogger(QueryRequestFederator.class);
	private static final AtomicLong QUERY_NUMBER = new AtomicLong();
	private final InternalClient internalClient;
	private final Collection<ZuliaIndex> indexes;
	private final Map<String, Query> queryMap;

	public QueryRequestFederator(Node thisNode, Collection<Node> otherNodesActive, MasterSlaveSettings masterSlaveSettings, Collection<ZuliaIndex> indexes,
			ExecutorService pool, InternalClient internalClient, Map<String, Query> queryMap) throws IOException {
		super(thisNode, otherNodesActive, masterSlaveSettings, indexes, pool);
		this.internalClient = internalClient;
		this.indexes = indexes;
		this.queryMap = queryMap;
	}

	@Override
	protected InternalQueryResponse processExternal(Node node, QueryRequest request) throws Exception {
		InternalQueryRequest internalQueryRequest = InternalQueryRequest.newBuilder().addAllIndexRouting(getIndexRouting(node)).setQueryRequest(request)
				.build();
		return internalClient.executeQuery(node, internalQueryRequest);
	}

	@Override
	protected InternalQueryResponse processInternal(Node node, QueryRequest request) throws Exception {
		InternalQueryRequest internalQueryRequest = InternalQueryRequest.newBuilder().addAllIndexRouting(getIndexRouting(node)).setQueryRequest(request)
				.build();
		return internalQuery(indexes, internalQueryRequest, queryMap);
	}

	public static InternalQueryResponse internalQuery(Collection<ZuliaIndex> indexes, InternalQueryRequest request, Map<String, Query> queryMap)
			throws Exception {
		InternalQueryResponse.Builder internalQueryResponseBuilder = InternalQueryResponse.newBuilder();
		for (ZuliaIndex index : indexes) {
			Query query = queryMap.get(index.getIndexName());
			IndexShardResponse isr = index.internalQuery(query, request);
			internalQueryResponseBuilder.addIndexShardResponse(isr);
		}
		return internalQueryResponseBuilder.build();
	}

	public QueryResponse getResponse(QueryRequest request) throws Exception {

		long queryId = QUERY_NUMBER.getAndIncrement();

		long start = System.currentTimeMillis();

		String queryJson = JsonFormat.printer().print(request);

		queryJson = queryJson.replace('\n', ' ').replaceAll("\\s+", " ");

		String searchLabel = request.getSearchLabel();

		if (searchLabel.isEmpty()) {
			LOG.info("Running id {} query {}", queryId, queryJson);
		}
		else {
			LOG.info("Running id {} with label {} query {}", queryId, searchLabel, queryJson);
		}

		List<InternalQueryResponse> results = send(request);

		long mergeStart = System.currentTimeMillis();

		QueryCombiner queryCombiner = new QueryCombiner(indexes, request, results);

		QueryResponse qr = queryCombiner.getQueryResponse();

		long end = System.currentTimeMillis();
		handleLog(queryId, searchLabel, request.getDebug(), qr, results, end - start, end - mergeStart);
		if (!queryCombiner.isShort()) {
			return qr;
		}
		else {
			if (!request.getFetchFull()) {
				QueryRequest newRequest = request.toBuilder().setFetchFull(true).build();
				return getResponse(newRequest);
			}
			throw new Exception("Full fetch request is short");
		}

	}

	private static void handleLog(long queryId, String searchLabel, boolean debug, QueryResponse qr, List<InternalQueryResponse> results, long time,
			long mergeTime) {
		String prefix = "Finished query";
		if (qr.getShardsQueried() == qr.getShardsPinned()) {
			prefix = "Finished query from pinned cache";
		}
		else if (qr.getShardsQueried() == qr.getShardsCached()) {
			prefix = "Finished query from cache";
		}

		String resultSize = String.format("%.2f", (qr.getSerializedSize() / 1024.0));
		String hits = qr.getResultsCount() + " of " + qr.getTotalHits();

		String debugInfo = "";
		if (debug && results.size() > 1) {
			int totalShardSize = results.stream().mapToInt(InternalQueryResponse::getSerializedSize).sum();
			debugInfo = String.format(" merging %d responses (%.2fKB, %dms)", results.size(), totalShardSize / 1024.0, mergeTime);
		}

		if (searchLabel.isEmpty()) {
			LOG.info("{} id {} returning {} hits{} with result size {}KB in {}ms", prefix, queryId, hits, debugInfo, resultSize, time);
		}
		else {
			LOG.info("{} id {} with label {} returning {} hits{} with result size {}KB in {}ms", prefix, queryId, searchLabel, hits, debugInfo, resultSize,
					time);
		}
	}
}

package io.zulia.signals.zulia;

import io.zulia.client.command.builder.Search;
import io.zulia.client.result.SearchResult;
import io.zulia.message.ZuliaQuery.FieldSort;
import io.zulia.message.ZuliaQuery.Query;
import io.zulia.message.ZuliaQuery.Query.QueryType;
import io.zulia.message.ZuliaQuery.ScoredResult;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.signals.model.Signal;
import io.zulia.signals.model.Actions;
import io.zulia.signals.model.SearchDetails;
import io.zulia.signals.model.Targets;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** The caller adds app, client, session, and actor. */
public final class ZuliaSearchSignals {

	private ZuliaSearchSignals() {
	}

	/** Latency is the result's {@link SearchResult#getCommandTimeMs()}. */
	public static Signal.Builder searchSignal(Search search, SearchResult result) {
		return searchSignal(search, result, Duration.ofMillis(result.getCommandTimeMs()));
	}

	/** The target is every index searched */
	public static Signal.Builder searchSignal(Search search, SearchResult result, Duration latency) {
		QueryRequest request = search.getRequest();
		List<String> indexes = request.getIndexList();
		List<String> shown = result.getUniqueIds().stream().limit(SearchDetails.MAX_SHOWN_DOC_IDS).toList();

		Signal.Builder builder = Signal.builder().action(Actions.SEARCH).target(Targets.INDEX, indexes)
				.search(sd -> sd.query(userQuery(request)).indexes(indexes).resultCount(result.getTotalHits()).latency(latency).shownDocIds(shown));

		builder.tag("start", request.getStart()).tag("amount", request.getAmount());
		putCount(builder, "filterClauses",
				countClauses(request, QueryType.FILTER, QueryType.FILTER_NOT, QueryType.NUMERIC_SET, QueryType.NUMERIC_SET_NOT,
						QueryType.TERMS, QueryType.TERMS_NOT));
		putCount(builder, "vectorClauses", countClauses(request, QueryType.VECTOR, QueryType.VECTOR_SHOULD));
		putCount(builder, "facets", request.getFacetRequest().getCountRequestCount());
		putList(builder, "queryFields", queryFields(request));
		putList(builder, "sort", request.getSortRequest().getFieldSortList().stream().map(FieldSort::getSortField).toList());
		if (!request.getSearchLabel().isEmpty()) {
			builder.tag("searchLabel", request.getSearchLabel());
		}
		return builder;
	}

	public static Signal.Builder click(Signal searchSignal, String docId, int position) {
		return linked(searchSignal, Actions.CLICK, docId, position);
	}

	/**
	 * A follow-up on a result, carrying the actor, session, and client of the search signal. It links through searchSignalId and
	 * searchId and carries no query text, so query facets count searches only.
	 */
	public static Signal.Builder linked(Signal searchSignal, String actionType, String docId, int position) {
		return linked(searchSignal, actionType, Targets.DOCUMENT, docId, position);
	}

	public static Signal.Builder linked(Signal searchSignal, String actionType, String targetType, String docId, int position) {
		if (searchSignal.search() == null) {
			throw new IllegalArgumentException(
					"Signal " + searchSignal.signalId() + " is a " + searchSignal.actionType() + " with no search details, so nothing can link to it");
		}
		SearchDetails origin = searchSignal.search();
		return Signal.builder().app(searchSignal.app()).client(searchSignal.client()).session(searchSignal.sessionId())
				.actor(searchSignal.actor()).action(actionType).target(targetType, docId)
				.search(sd -> sd.searchSignalId(searchSignal.signalId()).searchId(origin.searchId()).clickedPosition(position).clickedDocId(docId));
	}

	// TERMS is a filter on the server (Occur.FILTER), so a term list never reads as typed text
	private static String userQuery(QueryRequest request) {
		String query = request.getQueryList().stream().filter(q -> isScored(q.getQueryType())).map(Query::getQ).filter(text -> !text.isEmpty())
				.collect(Collectors.joining(" "));
		return query.isEmpty() ? null : query;
	}

	private static boolean isScored(QueryType type) {
		return switch (type) {
			case SCORE_MUST, SCORE_SHOULD, MORE_LIKE_THIS -> true;
			case FILTER, FILTER_NOT, NUMERIC_SET, NUMERIC_SET_NOT, TERMS, TERMS_NOT, VECTOR, VECTOR_SHOULD -> false;
			default -> false;
		};
	}

	private static int countClauses(QueryRequest request, QueryType... types) {
		Set<QueryType> wanted = Set.of(types);
		return (int) request.getQueryList().stream().filter(q -> wanted.contains(q.getQueryType())).count();
	}

	// filter fields are not query fields
	private static List<String> queryFields(QueryRequest request) {
		Set<String> fields = new LinkedHashSet<>();
		for (Query clause : request.getQueryList()) {
			if (isScored(clause.getQueryType())) {
				fields.addAll(clause.getQfList());
			}
		}
		return List.copyOf(fields);
	}

	private static void putCount(Signal.Builder builder, String key, int count) {
		if (count > 0) {
			builder.tag(key, count);
		}
	}

	private static void putList(Signal.Builder builder, String key, List<String> values) {
		if (!values.isEmpty()) {
			builder.tag(key, values);
		}
	}
}

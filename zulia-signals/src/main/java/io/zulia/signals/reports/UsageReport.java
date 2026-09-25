package io.zulia.signals.reports;

import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.TermQuery;
import io.zulia.client.command.factory.InstantRangeFilter;
import io.zulia.client.command.factory.RangeBehavior;
import io.zulia.client.result.SearchResult;
import io.zulia.message.ZuliaQuery.FacetCount;
import io.zulia.signals.client.SignalsClient;
import io.zulia.signals.client.SignalsException;
import io.zulia.signals.model.Actions;
import io.zulia.signals.model.ActorType;
import io.zulia.signals.storage.SignalField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/** One app over one range, from {@link UsageReports#of(String, TimeRange)}. SYSTEM actors are excluded unless {@link #includingSystem()}. */
public final class UsageReport {

	private final SignalsClient client;
	private final String app;
	private final TimeRange range;
	private final boolean includeSystem;
	private final List<Filter> filters;
	private final int maxFacetValues;
	private final int maxTallySearches;

	private record Filter(String fieldName, String value) {
	}

	UsageReport(SignalsClient client, String app, TimeRange range, int maxFacetValues, int maxTallySearches) {
		this(client, app, range, false, List.of(), maxFacetValues, maxTallySearches);
	}

	private UsageReport(SignalsClient client, String app, TimeRange range, boolean includeSystem, List<Filter> filters, int maxFacetValues,
			int maxTallySearches) {
		if (app == null || app.isBlank()) {
			throw new IllegalArgumentException("App is required for a usage report but was " + (app == null ? "null" : "blank"));
		}
		if (range == null) {
			throw new IllegalArgumentException("Time range is required for a usage report of app " + app + " but was null");
		}
		this.client = client;
		this.app = app;
		this.range = range;
		this.includeSystem = includeSystem;
		this.filters = filters;
		this.maxFacetValues = maxFacetValues;
		this.maxTallySearches = maxTallySearches;
	}

	/** The same report with SYSTEM actors counted, for seeing platform load next to usage. */
	public UsageReport includingSystem() {
		return new UsageReport(client, app, range, true, filters, maxFacetValues, maxTallySearches);
	}

	/** The same report narrowed to one activity. A report narrows to one activity once. */
	public UsageReport forActivity(Activity activity) {
		if (activity == null) {
			throw new IllegalArgumentException("Activity is required to narrow the report of " + app + " but was null");
		}
		Filter action = new Filter(SignalField.ACTION_TYPE.fieldName(), activity.actionType());
		return narrowedBy(
				activity.targetType() == null ? List.of(action) : List.of(action, new Filter(SignalField.TARGET_TYPE.fieldName(), activity.targetType())));
	}

	/** The same report narrowed to one actor. Takes the real actor id and maps it the way the client stored it. */
	public UsageReport forActor(String actorId) {
		requireText(actorId, "Actor id");
		return narrowedBy(List.of(new Filter(SignalField.ACTOR_ID.fieldName(), client.storedActorId(app, actorId))));
	}

	/** The same report narrowed to signals carrying the value in a keyword tag. A report narrows once per tag, different tags stack. */
	public UsageReport forTag(String tag, String value) {
		String field = indexedTagField(tag, SignalField.Kind.KEYWORD, SignalField.Kind.KEYWORD_FACET);
		requireText(value, "Value of tag " + tag);
		return narrowedBy(List.of(new Filter(field, value)));
	}

	/** The same report narrowed to one value of a built in keyword field, for example the client. */
	public UsageReport forField(SignalField field, String value) {
		if (field == null) {
			throw new IllegalArgumentException("Field is required to narrow the report of " + app + " but was null");
		}
		if (field.kind() != SignalField.Kind.KEYWORD && field.kind() != SignalField.Kind.KEYWORD_FACET) {
			throw new IllegalArgumentException(field + " (" + field.fieldName() + ") is " + field.kind() + ", only keyword fields narrow by value");
		}
		requireText(value, "Value of " + field.fieldName());
		return narrowedBy(List.of(new Filter(field.fieldName(), value)));
	}

	/**
	 * Signal counts per actor and activity, by total descending. Only actors with at least one tallied signal get a row. One search per activity,
	 * so the cost does not grow with the actors. Counts are signals, so a bulk counts once. Keep the activities disjoint, an activity without a
	 * target type overlaps every activity of that action and the total sums the columns.
	 */
	public List<ActorTally<Activity>> tally(List<Activity> activities) {
		if (activities == null || activities.isEmpty() || activities.stream().anyMatch(Objects::isNull)) {
			throw new IllegalArgumentException("Activities are required for a tally of " + app + " but were " + activities);
		}
		if (activities.stream().distinct().count() < activities.size()) {
			throw new IllegalArgumentException("Activities of a tally must be distinct but were " + activities);
		}
		Map<String, Map<Activity, Long>> countsByActor = new HashMap<>();
		for (Activity activity : activities) {
			List<DimensionCount> actors = forActivity(activity).by(SignalField.ACTOR_ID);
			if (actors.size() >= maxFacetValues) {
				throw new IllegalStateException("At least " + maxFacetValues + " actors did " + activity + " in app " + app + " over " + range
						+ ", the tally would drop some. Raise UsageReports.maxFacetValues to count them");
			}
			for (DimensionCount actor : actors) {
				countsByActor.computeIfAbsent(actor.value(), actorId -> new HashMap<>()).put(activity, actor.count());
			}
		}

		return rows(countsByActor);
	}

	public List<ActorTally<Activity>> tally(Activity... activities) {
		return tally(activities == null ? null : Arrays.asList(activities));
	}

	/**
	 * Signal counts per actor and value of an indexed keyword tag, by total descending. Narrow by {@link #forActivity(Activity)} first for one
	 * activity's breakdown. Runs one search per actor or per value, whichever there are fewer of, plus two to count them, so it refuses past
	 * {@link UsageReports#maxTallySearches()} searches. A list tag counts a signal once per element.
	 */
	public List<ActorTally<String>> tallyBy(String tag) {
		return tallyByField(indexedTagField(tag, SignalField.Kind.KEYWORD_FACET));
	}

	/** Same as {@link #tallyBy(String)} over a built in keyword facet field, for example the client or a time bucket. */
	public List<ActorTally<String>> tallyBy(SignalField field) {
		if (field == null) {
			throw new IllegalArgumentException("Field is required for a tally but was null");
		}
		if (field.kind() != SignalField.Kind.KEYWORD_FACET) {
			throw new IllegalArgumentException(field + " (" + field.fieldName() + ") is " + field.kind() + ", only KEYWORD_FACET fields tally by value");
		}
		return tallyByField(field.fieldName());
	}

	private List<ActorTally<String>> tallyByField(String field) {
		String actorField = SignalField.ACTOR_ID.fieldName();
		if (field.equals(actorField)) {
			throw new IllegalArgumentException("A tally by " + actorField + " is one column per actor, use by(SignalField.ACTOR_ID)");
		}
		for (Filter filter : filters) {
			if (filter.fieldName().equals(actorField) || filter.fieldName().equals(field)) {
				throw new IllegalArgumentException("Report of " + app + " is narrowed to " + filter.fieldName() + " " + filter.value()
						+ ", a tally by " + field + " needs every actor and value. Use by(...) on the narrowed report for one row or column");
			}
		}
		List<DimensionCount> values = byField(field);
		List<DimensionCount> actors = byField(actorField);
		if (values.size() >= maxFacetValues || actors.size() >= maxFacetValues) {
			throw new IllegalStateException("At least " + maxFacetValues + " " + (values.size() >= maxFacetValues ? field : actorField) + " values in app " + app
					+ " over " + range + ", the tally would drop some. Raise UsageReports.maxFacetValues to count them");
		}
		if (values.isEmpty() || actors.isEmpty()) {
			return List.of();
		}
		boolean perValue = values.size() <= actors.size();
		List<DimensionCount> loop = perValue ? values : actors;
		if (loop.size() > maxTallySearches) {
			throw new IllegalStateException("A tally by " + field + " in app " + app + " over " + range + " needs " + loop.size() + " searches ("
					+ values.size() + " values, " + actors.size() + " actors) but at most " + maxTallySearches
					+ " are allowed. Narrow the report, or raise UsageReports.maxTallySearches");
		}

		// counts[actor][value], filled one search per loop key faceting the other field
		Map<String, Map<String, Long>> countsByActor = new HashMap<>();
		for (DimensionCount key : loop) {
			UsageReport narrowed = narrowedBy(List.of(new Filter(perValue ? field : actorField, key.value())));
			for (DimensionCount count : narrowed.byField(perValue ? actorField : field)) {
				String actor = perValue ? count.value() : key.value();
				String value = perValue ? key.value() : count.value();
				countsByActor.computeIfAbsent(actor, id -> new HashMap<>()).put(value, count.count());
			}
		}

		return rows(countsByActor);
	}

	/** By total descending, then actor id. */
	private static <K> List<ActorTally<K>> rows(Map<String, Map<K, Long>> countsByActor) {
		List<ActorTally<K>> rows = new ArrayList<>();
		for (Map.Entry<String, Map<K, Long>> entry : countsByActor.entrySet()) {
			rows.add(new ActorTally<>(entry.getKey(), entry.getValue()));
		}
		rows.sort(Comparator.comparingLong((ActorTally<K> row) -> row.total()).reversed().thenComparing(ActorTally::actorId));
		return rows;
	}

	private UsageReport narrowedBy(List<Filter> added) {
		for (Filter filter : added) {
			filters.stream().filter(existing -> existing.fieldName().equals(filter.fieldName())).findFirst().ifPresent(existing -> {
				throw new IllegalArgumentException("Report of " + app + " is already narrowed to " + existing.fieldName() + " " + existing.value()
						+ ", it cannot also be narrowed to " + filter.value());
			});
		}
		List<Filter> narrowed = new ArrayList<>(filters);
		narrowed.addAll(added);
		return new UsageReport(client, app, range, includeSystem, List.copyOf(narrowed), maxFacetValues, maxTallySearches);
	}

	public String app() {
		return app;
	}

	public TimeRange range() {
		return range;
	}

	/** Distinct USER actors. */
	public long activeUsers() {
		return activeUsers(ActorType.USER);
	}

	/** Distinct actors of the given types, for example USER and ANONYMOUS together. SYSTEM needs {@link #includingSystem()}. */
	public long activeUsers(ActorType... types) {
		if (types == null || types.length == 0 || Arrays.asList(types).contains(null)) {
			throw new IllegalArgumentException("Actor types are required for active users of " + app + " but were " + Arrays.toString(types));
		}
		if (Arrays.asList(types).contains(ActorType.SYSTEM) && !includeSystem) {
			throw new IllegalArgumentException("SYSTEM actors are excluded from this report of " + app + ", use includingSystem() to count them");
		}
		String field = SignalField.ACTOR_ID.fieldName();
		TermQuery actorTypes = new TermQuery(SignalField.ACTOR_TYPE.fieldName()).addTerms(Arrays.stream(types).map(Enum::name).toList());
		return distinctCount(search(search -> search.addQuery(actorTypes).addCountFacet(topN(field))), field);
	}

	/** Counts per value of an indexed keyword tag, by count descending. Built-in fields go through {@link #by(SignalField)}. */
	public List<DimensionCount> by(String tag) {
		return byField(indexedTagField(tag, SignalField.Kind.KEYWORD_FACET));
	}

	private String indexedTagField(String tag, SignalField.Kind... allowed) {
		requireText(tag, "Tag key");
		SignalField.Kind kind = client.config().indexedTags().get(tag);
		if (kind == null) {
			throw new IllegalArgumentException("Tag " + tag + " is not indexed on " + client.config().indexName() + ", indexed tags are "
					+ client.config().indexedTags().keySet() + ". Add it with SignalsIndexConfig.indexTags. Built in fields take a SignalField instead");
		}
		if (!Arrays.asList(allowed).contains(kind)) {
			throw new IllegalArgumentException("Tag " + tag + " is indexed as " + kind + ", this needs one of " + Arrays.toString(allowed));
		}
		return SignalField.tagField(tag);
	}

	public List<DimensionCount> by(SignalField field) {
		if (field == null) {
			throw new IllegalArgumentException("Field is required but was null");
		}
		if (field.kind() != SignalField.Kind.KEYWORD_FACET) {
			throw new IllegalArgumentException(field + " (" + field.fieldName() + ") is " + field.kind() + ", only KEYWORD_FACET fields count by value");
		}
		return byField(field.fieldName());
	}

	/** Chronological. */
	public List<DimensionCount> overTime(Bucket bucket) {
		return by(bucket.field()).stream().sorted(Comparator.comparing(DimensionCount::value)).toList();
	}

	/** Normalized query text of SEARCH signals by frequency. Follow ups carry no query text, so they never inflate this. */
	public List<DimensionCount> topQueries() {
		String field = SignalField.SEARCH_QUERY_NORMALIZED.fieldName();
		return counts(search(search -> search.addQuery(searches()).addCountFacet(topN(field))), field);
	}

	/** Normalized query text of SEARCH signals that found nothing, by frequency. */
	public List<DimensionCount> zeroResultQueries() {
		String field = SignalField.SEARCH_QUERY_NORMALIZED.fieldName();
		FilterQuery noHits = new FilterQuery(SignalField.SEARCH_RESULT_COUNT.fieldName() + ":[0 TO 0]");
		return counts(search(search -> search.addQuery(searches()).addQuery(noHits).addCountFacet(topN(field))), field);
	}

	/** Distinct targets of a type touched by any action. */
	public long distinct(String targetType) {
		requireText(targetType, "Target type");
		return distinctCount(search(search -> targets(search, targetType)), SignalField.TARGET_ID.fieldName());
	}

	/** Distinct targets of a type touched by one action. */
	public long distinct(String targetType, String actionType) {
		requireText(targetType, "Target type");
		if (actionType == null || actionType.isBlank()) {
			throw new IllegalArgumentException("Action type is required here but was " + (actionType == null ? "null" : "blank")
					+ ", use distinct(targetType) to count across all actions");
		}
		TermQuery action = createTermQuery(SignalField.ACTION_TYPE, actionType);
		return distinctCount(search(search -> targets(search, targetType).addQuery(action)), SignalField.TARGET_ID.fieldName());
	}

	private List<DimensionCount> byField(String field) {
		return counts(search(search -> search.addCountFacet(topN(field))), field);
	}

	private SearchResult search(Consumer<Search> report) {
		return rangeSearch(client, range, includeSystem, search -> {
			search.addQuery(createTermQuery(SignalField.APP, app));
			filters.forEach(filter -> search.addQuery(new TermQuery(filter.fieldName()).addTerm(filter.value())));
			report.accept(search);
		});
	}

	// a distinct count is the size of a facet, so past the facet limit it would silently truncate
	private long distinctCount(SearchResult result, String field) {
		List<FacetCount> counts = facetCounts(result, field);
		if (counts.size() >= maxFacetValues) {
			throw new IllegalStateException("At least " + maxFacetValues + " distinct " + field + " values for app " + app + " in " + range
					+ ", the count would be truncated. Raise UsageReports.maxFacetValues to count them");
		}
		return counts.size();
	}

	private static TermQuery searches() {
		return createTermQuery(SignalField.ACTION_TYPE, Actions.SEARCH);
	}

	private Search targets(Search search, String targetType) {
		return search.addQuery(createTermQuery(SignalField.TARGET_TYPE, targetType))
				.addCountFacet(topN(SignalField.TARGET_ID.fieldName()));
	}

	private static void requireText(String value, String what) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(what + " is required but was " + (value == null ? "null" : "blank"));
		}
	}

	static SearchResult rangeSearch(SignalsClient client, TimeRange range, boolean includeSystem, Consumer<Search> report) {
		try {
			return client.search(search -> {
				search.setAmount(0);
				search.addQuery(new InstantRangeFilter(SignalField.TIMESTAMP.fieldName()).setRange(range.from(), range.to())
						.setEndpointBehavior(RangeBehavior.INCLUDE_MIN));
				if (!includeSystem) {
					search.addQuery(createTermQuery(SignalField.ACTOR_TYPE, ActorType.SYSTEM.name()).exclude());
				}
				report.accept(search);
			});
		}
		catch (Exception e) {
			throw new SignalsException("Usage report search on " + client.config().readName() + " over " + range + " failed", e);
		}
	}

	private CountFacet topN(String field) {
		return topN(field, maxFacetValues);
	}

	static CountFacet topN(String field, int maxFacetValues) {
		return new CountFacet(field).setTopN(maxFacetValues);
	}

	static List<DimensionCount> counts(SearchResult result, String field) {
		return facetCounts(result, field).stream().map(count -> new DimensionCount(count.getFacet(), count.getCount())).toList();
	}

	// a TERMS query matches the stored keyword directly and never goes through the query parser, so values need no escaping
	private static TermQuery createTermQuery(SignalField field, String value) {
		return new TermQuery(field.fieldName()).addTerm(value);
	}

	private static List<FacetCount> facetCounts(SearchResult result, String field) {
		List<FacetCount> counts = result.getFacetCounts(field);
		return counts == null ? List.of() : counts;
	}
}

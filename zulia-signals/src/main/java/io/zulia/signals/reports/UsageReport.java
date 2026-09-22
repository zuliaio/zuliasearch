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

	private record Filter(SignalField field, String value) {
	}

	UsageReport(SignalsClient client, String app, TimeRange range, int maxFacetValues) {
		this(client, app, range, false, List.of(), maxFacetValues);
	}

	private UsageReport(SignalsClient client, String app, TimeRange range, boolean includeSystem, List<Filter> filters, int maxFacetValues) {
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
	}

	/** The same report with SYSTEM actors counted, for seeing platform load next to usage. */
	public UsageReport includingSystem() {
		return new UsageReport(client, app, range, true, filters, maxFacetValues);
	}

	/** The same report narrowed to one activity. A report narrows to one activity once. */
	public UsageReport forActivity(Activity activity) {
		if (activity == null) {
			throw new IllegalArgumentException("Activity is required to narrow the report of " + app + " but was null");
		}
		Filter action = new Filter(SignalField.ACTION_TYPE, activity.actionType());
		return narrowedBy(activity.targetType() == null ? List.of(action) : List.of(action, new Filter(SignalField.TARGET_TYPE, activity.targetType())));
	}

	/** The same report narrowed to one actor. Takes the real actor id and maps it the way the client stored it. */
	public UsageReport forActor(String actorId) {
		requireText(actorId, "Actor id");
		return narrowedBy(List.of(new Filter(SignalField.ACTOR_ID, client.storedActorId(app, actorId))));
	}

	/**
	 * Signal counts per actor and activity, by total descending. Only actors with at least one tallied signal get a row. One search per activity,
	 * so the cost does not grow with the actors. Counts are signals, so a bulk counts once. Keep the activities disjoint, an activity without a
	 * target type overlaps every activity of that action and the total sums the columns.
	 */
	public List<ActorActivity> tally(List<Activity> activities) {
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

		List<ActorActivity> list = new ArrayList<>();
		for (Map.Entry<String, Map<Activity, Long>> entry : countsByActor.entrySet()) {
			ActorActivity actorActivity = new ActorActivity(entry.getKey(), entry.getValue());
			list.add(actorActivity);
		}
		list.sort(Comparator.comparingLong(ActorActivity::total).reversed().thenComparing(ActorActivity::actorId));
		return list;
	}

	public List<ActorActivity> tally(Activity... activities) {
		return tally(activities == null ? null : Arrays.asList(activities));
	}

	private UsageReport narrowedBy(List<Filter> added) {
		for (Filter filter : added) {
			filters.stream().filter(existing -> existing.field() == filter.field()).findFirst().ifPresent(existing -> {
				throw new IllegalArgumentException("Report of " + app + " is already narrowed to " + existing.field().fieldName() + " " + existing.value()
						+ ", it cannot also be narrowed to " + filter.value());
			});
		}
		List<Filter> narrowed = new ArrayList<>(filters);
		narrowed.addAll(added);
		return new UsageReport(client, app, range, includeSystem, List.copyOf(narrowed), maxFacetValues);
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

	/** Counts per value of a keyword facet dimension, by count descending. Built-in fields go through {@link #by(SignalField)}. */
	public List<DimensionCount> by(String dimension) {
		if (dimension == null || dimension.isBlank()) {
			throw new IllegalArgumentException("Dimension is required but was " + (dimension == null ? "null" : "blank"));
		}
		SignalField.Kind kind = client.config().dimensions().get(dimension);
		if (kind == null) {
			throw new IllegalArgumentException("Dimension " + dimension + " is not declared on " + client.config().indexName() + ", declared dimensions are "
					+ client.config().dimensions().keySet() + ". Built in fields go through by(SignalField)");
		}
		if (kind != SignalField.Kind.KEYWORD_FACET) {
			throw new IllegalArgumentException("Dimension " + dimension + " is " + kind + ", only KEYWORD_FACET dimensions count by value");
		}
		return byField(SignalField.tagField(dimension));
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
			filters.forEach(filter -> search.addQuery(createTermQuery(filter.field(), filter.value())));
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

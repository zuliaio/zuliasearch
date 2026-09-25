package io.zulia.signals;

import io.zulia.client.command.DeleteIndex;
import io.zulia.client.command.DeleteIndexAlias;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.TermQuery;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.server.test.node.shared.FsNodeExtension;
import io.zulia.signals.client.RecordFailurePolicy;
import io.zulia.signals.client.RetentionResult;
import io.zulia.signals.client.SignalsClient;
import io.zulia.signals.client.SignalsRetention;
import io.zulia.signals.model.Actions;
import io.zulia.signals.model.Actor;
import io.zulia.signals.model.ActorType;
import io.zulia.signals.model.Signal;
import io.zulia.signals.model.Targets;
import io.zulia.signals.reports.Activity;
import io.zulia.signals.reports.ActorTally;
import io.zulia.signals.reports.Bucket;
import io.zulia.signals.reports.DimensionCount;
import io.zulia.signals.reports.TimeRange;
import io.zulia.signals.reports.UsageReport;
import io.zulia.signals.reports.UsageReports;
import io.zulia.signals.storage.ActorIdMapper;
import io.zulia.signals.storage.SignalEnricher;
import io.zulia.signals.storage.SignalField;
import io.zulia.signals.storage.SignalsIndexConfig;
import io.zulia.signals.zulia.ZuliaSearchSignals;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Clock;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * End to end against a single node started in this JVM (filesystem mode, no MongoDB). Own ports so it can run alongside
 * zulia-server's tests under a parallel build.
 */
class SignalsNodeTest {

	@RegisterExtension
	static final FsNodeExtension fsNode = new FsNodeExtension(21291, 21292, null);

	private static ZuliaWorkPool pool;
	private static String suffix;

	@BeforeAll
	static void connect() {
		pool = fsNode.getClient();
		suffix = Long.toString(System.currentTimeMillis(), 36);
	}

	private static void refresh(String indexName) throws Exception {
		pool.search(new Search(indexName).setAmount(1).setRealtime(true));
	}

	private static Signal view(String app, Actor actor, Instant at) {
		return Signal.builder().app(app).client("web").session("s-" + actor.id()).actor(actor).action(Actions.VIEW).timestamp(at).target(Targets.PAGE, "home")
				.build();
	}

	private static Signal projectSignal(String action, Actor actor, String projectId, String division, Instant at) {
		return Signal.builder().app("curation-app").client("web").session("s-" + actor.id()).actor(actor).action(action).timestamp(at)
				.target(Targets.PROJECT, projectId).tag("division", division).build();
	}

	@Test
	void singleIndexRoundTrip() throws Exception {
		SignalsIndexConfig config = SignalsIndexConfig.defaults().indexName("sigtest-" + suffix).indexTags("division", "labels")
				.indexTag("records", SignalField.Kind.LONG).indexTag("ticket", SignalField.Kind.KEYWORD);
		SignalsClient signals = new SignalsClient(pool, config);
		try {
			signals.ensureStorage();
			signals.ensureStorage();
			Assertions.assertTrue(pool.getIndexes().containsIndex(config.indexName()));

			Instant now = Instant.now();
			Instant twoDaysAgo = now.minus(Duration.ofDays(2));

			Search search = new Search(config.indexName()).setAmount(5);
			search.addQuery(new io.zulia.client.command.builder.ScoredQuery("home"));
			Signal searchSignal = ZuliaSearchSignals.searchSignal(search, pool.search(search)).app("search-app").client("web").session("s-u1")
					.actor(Actor.user("u1")).searchId("saved-7").build();
			Assertions.assertTrue(searchSignal.search().latencyMs() >= 0, "latency from commandTimeMs");
			signals.recordAll(List.of(searchSignal, ZuliaSearchSignals.click(searchSignal, "doc-1", 0).build(), view("search-app", Actor.user("u2"), now),
					view("search-app", Actor.user("u1"), twoDaysAgo), view("search-app", Actor.agent("zulia-agent", "u2"), twoDaysAgo),
					view("search-app", Actor.system(), now), view("curation-app", Actor.user("u3"), now),
					projectSignal(Actions.CREATE, Actor.user("u3"), "p1", "north", now), projectSignal(Actions.VISIT, Actor.user("u3"), "p2", "south", now),
					projectSignal(Actions.VISIT, Actor.user("u4"), "p2", "south", now),
					Signal.builder().app("curation-app").client("web").session("s-u3").actor(Actor.user("u3")).action(Actions.LOGOUT).timestamp(now)
							.duration(Duration.ofMinutes(25)).tag("records", 12).tag("labels", List.of("alpha", "beta")).tag("ticket", "T-1").build(),
					Signal.builder().app("portal-app").client("web").session("s-u5").actor(Actor.user("u5")).action(Actions.SEARCH).timestamp(now)
							.target(Targets.INDEX, List.of("idx-a", "idx-b")).search(sd -> sd.query("home").indexes("idx-a", "idx-b").resultCount(0)).build(),
					Signal.builder().app("curation-app").client("web").session("s-u3").actor(Actor.user("u3")).action(Actions.ANNOTATE).timestamp(now)
							.target(Targets.RECORD, List.of("r1", "r2", "r1", "r3")).build()));
			refresh(config.indexName());

			UsageReports reports = new UsageReports(signals);
			TimeRange lastWeek = new TimeRange(now.minus(Duration.ofDays(7)), now.plus(Duration.ofMinutes(1)));

			List<DimensionCount> byApp = reports.byApp(lastWeek);
			Assertions.assertEquals(5, usage(byApp, "search-app"), "five signals, system excluded: " + byApp);
			Assertions.assertEquals(6, usage(byApp, "curation-app"), "six signals: " + byApp);

			List<DimensionCount> days = reports.of("search-app", lastWeek).overTime(Bucket.DAY);
			Assertions.assertEquals(5, days.stream().mapToLong(DimensionCount::count).sum(), days.toString());
			Assertions.assertEquals(days.stream().map(DimensionCount::value).sorted().toList(), days.stream().map(DimensionCount::value).toList(),
					"chronological");
			Assertions.assertTrue(days.size() >= 2 && days.size() <= 3, "two or three days: " + days);

			Assertions.assertEquals(2, reports.of("search-app", lastWeek).activeUsers(), "u1 and u2 only");
			Assertions.assertEquals(3, reports.of("search-app", lastWeek).activeUsers(ActorType.USER, ActorType.AGENT), "the agent counts when asked");
			Assertions.assertThrows(IllegalArgumentException.class, () -> reports.of("search-app", lastWeek).activeUsers(ActorType.SYSTEM),
					"excluded by default");
			UsageReport withSystem = reports.of("search-app", lastWeek).includingSystem();
			Assertions.assertEquals(1, withSystem.activeUsers(ActorType.SYSTEM), "the system view counts when included");
			Assertions.assertTrue(withSystem.by(SignalField.ACTOR_TYPE).contains(new DimensionCount("SYSTEM", 1)), "platform load shows next to usage");
			Assertions.assertEquals(6,
					usage(List.of(new DimensionCount("search-app", withSystem.by(SignalField.ACTION_TYPE).stream().mapToLong(DimensionCount::count).sum())),
							"search-app"), "six signals including the system view");
			Assertions.assertThrows(IllegalArgumentException.class, () -> reports.of("search-app", lastWeek).by(SignalField.SESSION_ID), "not a facet");
			Assertions.assertEquals(List.of(new DimensionCount("home", 1)), reports.of("search-app", lastWeek).topQueries(), "the click adds no query");
			Assertions.assertEquals(List.of(new DimensionCount("home", 1)), reports.of("search-app", lastWeek).zeroResultQueries(),
					"an empty index finds nothing");

			Assertions.assertEquals(2, reports.of("curation-app", lastWeek).distinct(Targets.PROJECT), "p1 and p2");
			Assertions.assertEquals(1, reports.of("curation-app", lastWeek).distinct(Targets.PROJECT, Actions.CREATE), "p1 only");
			Assertions.assertThrows(IllegalArgumentException.class, () -> reports.of("curation-app", lastWeek).distinct(Targets.PROJECT, " "));
			Assertions.assertEquals(3, reports.of("curation-app", lastWeek).distinct(Targets.RECORD, Actions.ANNOTATE), "a bulk counts each record once");
			Assertions.assertEquals(3, reports.maxFacetValues(4).of("curation-app", lastWeek).distinct(Targets.RECORD, Actions.ANNOTATE), "under the cap");
			Assertions.assertThrows(IllegalStateException.class, () -> reports.maxFacetValues(3).of("curation-app", lastWeek).distinct(Targets.RECORD, Actions.ANNOTATE),
					"a distinct count at the cap would be truncated");
			Assertions.assertEquals(List.of(new DimensionCount("u3", 5)), reports.maxFacetValues(1).of("curation-app", lastWeek).by(SignalField.ACTOR_ID),
					"a grouped report just truncates to the top values");
			Assertions.assertThrows(IllegalArgumentException.class, () -> reports.maxFacetValues(0));
			Assertions.assertTrue(reports.of("curation-app", lastWeek).by(SignalField.ACTION_TYPE).contains(new DimensionCount(Actions.ANNOTATE, 1)),
					"and the action once");

			Activity projectsCreated = Activity.of(Actions.CREATE, Targets.PROJECT);
			Activity projectsVisited = Activity.of(Actions.VISIT, Targets.PROJECT);
			Activity recordsCoded = Activity.of(Actions.ANNOTATE, Targets.RECORD);
			Activity logouts = Activity.of(Actions.LOGOUT);
			UsageReport curation = reports.of("curation-app", lastWeek);
			List<ActorTally<Activity>> tally = curation.tally(projectsCreated, projectsVisited, recordsCoded, logouts);
			Assertions.assertEquals(List.of("u3", "u4"), tally.stream().map(ActorTally::actorId).toList(), "by total descending: " + tally);
			ActorTally<Activity> u3 = tally.getFirst();
			Assertions.assertEquals(1, u3.count(projectsCreated));
			Assertions.assertEquals(1, u3.count(projectsVisited));
			Assertions.assertEquals(1, u3.count(recordsCoded), "a bulk is one signal");
			Assertions.assertEquals(1, u3.count(logouts), "an activity without a target type");
			Assertions.assertEquals(4, u3.total(), "the page view is not a tallied activity");
			Assertions.assertEquals(0, tally.getLast().count(projectsCreated), "u4 only visited");
			Assertions.assertEquals(List.of(new DimensionCount("u3", 1), new DimensionCount("u4", 1)),
					curation.forActivity(projectsVisited).by(SignalField.ACTOR_ID).stream().sorted(Comparator.comparing(DimensionCount::value)).toList());
			Assertions.assertEquals(List.of(new ActorTally<>("u4", Map.of(projectsVisited, 1L))), curation.forActor("u4").tally(projectsCreated, projectsVisited),
					"one actor's row");
			Assertions.assertEquals(3, curation.forActor("u3").distinct(Targets.RECORD, Actions.ANNOTATE), "distinct records for one actor");
			Assertions.assertEquals(0, curation.forActor("u4").distinct(Targets.RECORD, Actions.ANNOTATE));
			Assertions.assertEquals(1, curation.forActor("u3").activeUsers());
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.tally(List.of()));
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.tally(logouts, logouts), "duplicate activities");
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.forActor(" "));
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.forActivity(projectsVisited).tally(projectsCreated),
					"a report narrows to one activity once");
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.forActor("u3").forActor("u4"), "and to one actor once");
			Assertions.assertThrows(IllegalStateException.class, () -> reports.maxFacetValues(2).of("curation-app", lastWeek).tally(projectsVisited),
					"two visitors at a cap of two would truncate");
			Assertions.assertEquals(List.of("u3", "u4"), reports.maxFacetValues(3).of("curation-app", lastWeek).tally(projectsVisited).stream()
					.map(ActorTally::actorId).toList(), "under the cap");
			Activity pageViews = Activity.of(Actions.VIEW, Targets.PAGE);
			Assertions.assertTrue(reports.of("search-app", lastWeek).includingSystem().tally(pageViews).stream().map(ActorTally::actorId).toList()
					.contains(Actor.SYSTEM_ID), "the system view tallies when included");
			Assertions.assertFalse(reports.of("search-app", lastWeek).tally(pageViews).stream().map(ActorTally::actorId).toList().contains(Actor.SYSTEM_ID),
					"and not by default");

			String quotedApp = "data \"coding\" app";
			SignalsClient pseudonymous = new SignalsClient(pool, config, ActorIdMapper.hmacSha256("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8)));
			pseudonymous.record(view(quotedApp, Actor.user("u9"), now));
			refresh(config.indexName());
			UsageReport quoted = new UsageReports(pseudonymous).of(quotedApp, lastWeek);
			String pseudonym = pseudonymous.storedActorId(quotedApp, "u9");
			Assertions.assertNotEquals("u9", pseudonym);
			Assertions.assertEquals(1, quoted.forActor("u9").activeUsers(), "the real id filters through the mapper, the app name needs no escaping");
			Assertions.assertEquals(List.of(new ActorTally<>(pseudonym, Map.of(pageViews, 1L))), quoted.tally(pageViews), "rows carry the pseudonym");

			List<String> sinceYesterday = reports.of("search-app", LocalDate.now(config.zone()).minusDays(1)).by(SignalField.ACTOR_ID).stream()
					.map(DimensionCount::value).toList();
			Assertions.assertTrue(sinceYesterday.contains("u2") && !sinceYesterday.contains("zulia-agent"), "since yesterday drops the views from two days ago: " + sinceYesterday);
			Assertions.assertTrue(reports.of("search-app", lastWeek).by(SignalField.ACTOR_ID).stream().map(DimensionCount::value).toList().contains("zulia-agent"),
					"the week keeps them");
			Assertions.assertEquals(List.of(), reports.of("curation-app", YearMonth.now(config.zone()).plusMonths(1)).by("division"), "next month is empty");
			List<DimensionCount> byDivision = reports.of("curation-app", lastWeek).by("division");
			Assertions.assertEquals(List.of(new DimensionCount("alpha", 1), new DimensionCount("beta", 1)), reports.of("curation-app", lastWeek).by("labels"),
					"a list tag facets once per element");
			Assertions.assertEquals(1, signals.search(query -> query.setAmount(0).addQuery(new FilterQuery("tags.records:[10 TO 100]"))).getTotalHits(),
					"numeric tag filters through the dotted field");
			Assertions.assertEquals(List.of(new DimensionCount("south", 2), new DimensionCount("north", 1)), byDivision, "facets on an indexed tag");
			Assertions.assertThrows(IllegalArgumentException.class, () -> reports.of("curation-app", lastWeek).by("host"), "a tag that is not indexed");
			Assertions.assertThrows(IllegalArgumentException.class, () -> reports.of("curation-app", lastWeek).by("records"), "a numeric tag");

			UsageReport south = curation.forTag("division", "south");
			Assertions.assertEquals(List.of(new DimensionCount("u3", 1), new DimensionCount("u4", 1)),
					south.by(SignalField.ACTOR_ID).stream().sorted(Comparator.comparing(DimensionCount::value)).toList(), "narrowed to one tag value");
			Assertions.assertEquals(List.of("u3", "u4"), south.tally(projectsCreated, projectsVisited).stream().map(ActorTally::actorId).toList());
			Assertions.assertEquals(0, south.tally(projectsCreated, projectsVisited).getFirst().count(projectsCreated), "p1 was created in the north");
			Assertions.assertEquals(1, south.forActivity(projectsVisited).forActor("u4").activeUsers(), "tag, activity, and actor narrowings stack");
			Assertions.assertThrows(IllegalArgumentException.class, () -> south.forTag("division", "north"), "a report narrows to one value per tag");
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.forTag("division", " "));
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.forTag("host", "node-1"), "a tag that is not indexed");
			Assertions.assertEquals(List.of(new DimensionCount(Actions.LOGOUT, 1)), curation.forTag("ticket", "T-1").by(SignalField.ACTION_TYPE),
					"a keyword tag filters without being a facet");
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.by("ticket"), "but cannot be counted by value");
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.forTag("records", "12"), "a numeric tag is not a term");

			List<ActorTally<String>> byDivisionPerActor = curation.tallyBy("division");
			Assertions.assertEquals(List.of(new ActorTally<>("u3", Map.of("north", 1L, "south", 1L)), new ActorTally<>("u4", Map.of("south", 1L))), byDivisionPerActor,
					"two values and two actors loops the values, by total descending");
			Assertions.assertEquals(2, byDivisionPerActor.getFirst().total());
			Assertions.assertEquals(0, byDivisionPerActor.getLast().count("north"));
			Assertions.assertEquals(List.of(new ActorTally<>("u3", Map.of("south", 1L)), new ActorTally<>("u4", Map.of("south", 1L))),
					curation.forActivity(projectsVisited).tallyBy("division"), "one activity's breakdown");
			Assertions.assertEquals(List.of(new ActorTally<>("u3", Map.of("alpha", 1L, "beta", 1L))), curation.tallyBy("labels"), "a list tag counts once per element");
			Assertions.assertEquals(List.of(new ActorTally<>("u3",
							Map.of(Actions.VIEW, 1L, Actions.CREATE, 1L, Actions.VISIT, 1L, Actions.LOGOUT, 1L, Actions.ANNOTATE, 1L)),
							new ActorTally<>("u4", Map.of(Actions.VISIT, 1L))), curation.tallyBy(SignalField.ACTION_TYPE),
					"five values and two actors loops the actors, over a built in field");
			Assertions.assertEquals(List.of(), curation.forActivity(Activity.of(Actions.EXPORT)).tallyBy("division"), "nothing to tally");
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.forActor("u3").tallyBy("division"), "one actor is a by, not a tally");
			Assertions.assertThrows(IllegalArgumentException.class, () -> south.tallyBy("division"), "one value is a by, not a tally");
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.tallyBy("records"), "a numeric tag");
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.tallyBy("host"), "a tag that is not indexed");
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.tallyBy(SignalField.DURATION_MS), "a sortable field");
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.tallyBy(SignalField.ACTOR_ID), "the row key is not a column");
			Assertions.assertThrows(IllegalStateException.class, () -> reports.maxFacetValues(2).of("curation-app", lastWeek).tallyBy("division"),
					"two actors at a cap of two would truncate");
			Assertions.assertEquals(byDivisionPerActor, reports.maxFacetValues(3).of("curation-app", lastWeek).tallyBy("division"), "under the cap");
			IllegalStateException tooManySearches = Assertions.assertThrows(IllegalStateException.class,
					() -> reports.maxTallySearches(1).of("curation-app", lastWeek).tallyBy("division"), "two values need two searches");
			Assertions.assertTrue(tooManySearches.getMessage().contains("2 values, 2 actors"), tooManySearches.getMessage());
			Assertions.assertEquals(byDivisionPerActor, reports.maxTallySearches(2).of("curation-app", lastWeek).tallyBy("division"), "at the cap");
			Assertions.assertThrows(IllegalArgumentException.class, () -> reports.maxTallySearches(0));
			Assertions.assertEquals(List.of(new DimensionCount("u3", 5), new DimensionCount("u4", 1)), curation.forField(SignalField.CLIENT, "web").by(SignalField.ACTOR_ID),
					"narrowed by a built in keyword field");
			Assertions.assertEquals(List.of(), curation.forField(SignalField.CLIENT, "mobile").by(SignalField.ACTOR_ID));
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.forField(SignalField.DURATION_MS, "1"), "not a keyword field");
			Assertions.assertThrows(IllegalArgumentException.class, () -> curation.forField(SignalField.CLIENT, " "));
			Assertions.assertEquals(List.of(new ActorTally<>(pseudonym, Map.of("web", 1L))), quoted.tallyBy(SignalField.CLIENT), "rows carry the pseudonym");
			Assertions.assertEquals(0, reports.of("search-app", new TimeRange(now.minus(Duration.ofDays(30)), now.minus(Duration.ofDays(20)))).activeUsers());

			Signal stamped = view("search-app", Actor.user("u8"), now);
			signals.stamping(builder -> builder.tag("host", "node-1")).record(stamped);
			refresh(config.indexName());
			Document stored = signals.search(query -> query.setAmount(1).addQuery(new TermQuery(SignalField.SIGNAL_ID.fieldName()).addTerm(stamped.signalId())))
					.getFirstDocument();
			Assertions.assertEquals("node-1", stored.get(SignalField.TAGS, Document.class).getString("host"), "the stamp lands on the stored signal");

			Search bySearchId = new Search(config.indexName()).setAmount(0);
			bySearchId.addQuery(new TermQuery(SignalField.SEARCH_ID.fieldName()).addTerm("saved-7"));
			Assertions.assertEquals(2, pool.search(bySearchId).getTotalHits(), "search and click share searchId");

			List<DimensionCount> byIndex = reports.of("portal-app", lastWeek).by(SignalField.SEARCH_INDEX);
			Assertions.assertEquals(List.of(new DimensionCount("idx-a", 1), new DimensionCount("idx-b", 1)), byIndex, "one row per index");

			RetentionResult retained = new SignalsRetention(signals).deleteBefore(now.minus(Duration.ofDays(1)));
			Assertions.assertEquals(2, retained.signalsDeleted(), "two signals older than a day");
			refresh(config.indexName());
			Assertions.assertEquals(4, usage(reports.byApp(lastWeek), "search-app"));
		}
		finally {
			pool.deleteIndex(new DeleteIndex(config.indexName()));
		}
	}

	@Test
	void partitionedRoundTrip() throws Exception {
		SignalsIndexConfig config = SignalsIndexConfig.defaults().indexName("sigpart-" + suffix).monthlyPartitions();
		SettableClock clock = new SettableClock(Instant.now());
		SignalsClient signals = new SignalsClient(pool, config, ActorIdMapper.identity(), clock);
		YearMonth thisMonth = YearMonth.from(clock.instant().atZone(ZoneOffset.UTC));
		String next = config.partitionName(thisMonth.plusMonths(1));
		String current = config.partitionName(thisMonth);
		String previous = config.partitionName(thisMonth.minusMonths(1));
		try {
			signals.ensureStorage();
			Assertions.assertEquals(List.of(current), signals.existingPartitions());

			Instant now = clock.instant();
			signals.recordAll(List.of(view("search-app", Actor.user("u1"), now), view("search-app", Actor.user("u2"), now)));
			refresh(config.readName());
			TimeRange today = new TimeRange(now.minus(Duration.ofHours(1)), now.plus(Duration.ofMinutes(1)));
			Assertions.assertEquals(2, usage(new UsageReports(signals).byApp(today), "search-app"), "writes land in the current month");

			signals.ensurePartition(thisMonth.minusMonths(1));
			signals.ensurePartition(thisMonth.minusMonths(1));
			Assertions.assertEquals(List.of(previous, current), signals.existingPartitions(), "backfill month added once");
			Assertions.assertThrows(IllegalArgumentException.class, () -> signals.dropPartition(config.partitionName(thisMonth.minusMonths(2))),
					"not a member");

			Store backfill = new Store("backfilled-1", previous);
			backfill.setResultDocument(new SignalEnricher(config, clock).toDocument(view("search-app", Actor.user("u1"), now.minus(Duration.ofDays(40)))));
			pool.store(backfill);
			RetentionResult retained = new SignalsRetention(signals).deleteBefore(now);
			Assertions.assertEquals(List.of(previous), retained.partitionsDropped());
			Assertions.assertEquals(1, retained.signalsDeleted(), "the dropped month's signals are counted");
			Assertions.assertEquals(List.of(current), signals.existingPartitions());
			Assertions.assertEquals(2, usage(new UsageReports(signals).byApp(today), "search-app"), "current month survives retention");
			Assertions.assertThrows(IllegalStateException.class, () -> signals.dropPartition(current), "the write index is refused");

			clock.advance(Duration.ofDays(32));
			Assertions.assertTrue(signals.record(view("search-app", Actor.user("u9"), clock.instant())).accepted(),
					"the first record of a new month rolls over");
			Assertions.assertEquals(List.of(current, next), signals.existingPartitions(), "next month joined the alias");
			Assertions.assertEquals(next,
					pool.getIndexAliases().stream().filter(a -> a.getAliasName().equals(config.readName())).findFirst().orElseThrow().getWriteIndex(),
					"writes moved to the new month");
		}
		finally {
			pool.deleteIndexAlias(new DeleteIndexAlias(config.readName()));
			for (String partition : List.of(current, previous, next)) {
				if (pool.getIndexes().containsIndex(partition)) {
					pool.deleteIndex(new DeleteIndex(partition));
				}
			}
		}
	}

	@Test
	void recordCreatesStorageWhenSetupNeverRan() throws Exception {
		SignalsIndexConfig config = SignalsIndexConfig.defaults().indexName("sigheal-" + suffix);
		SignalsClient signals = new SignalsClient(pool, config).onFailure(RecordFailurePolicy.LOG_AND_DROP);
		try {
			Assertions.assertTrue(signals.record(view("search-app", Actor.user("u1"), Instant.now())).accepted(), "queued for the writer");
			signals.close();
			Assertions.assertTrue(pool.getIndexes().containsIndex(config.indexName()), "the writer created storage on first use");
			Assertions.assertEquals(0, signals.droppedSignals());
		}
		finally {
			pool.deleteIndex(new DeleteIndex(config.indexName()));
		}
	}

	private static long usage(List<DimensionCount> byApp, String app) {
		return byApp.stream().filter(u -> u.value().equals(app)).mapToLong(DimensionCount::count).sum();
	}

	private static final class SettableClock extends Clock {

		private Instant now;

		SettableClock(Instant now) {
			this.now = now;
		}

		void advance(Duration duration) {
			now = now.plus(duration);
		}

		@Override
		public ZoneId getZone() {
			return ZoneOffset.UTC;
		}

		@Override
		public Clock withZone(ZoneId zone) {
			return this;
		}

		@Override
		public Instant instant() {
			return now;
		}
	}
}

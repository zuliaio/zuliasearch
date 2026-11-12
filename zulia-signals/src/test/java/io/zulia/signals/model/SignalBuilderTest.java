package io.zulia.signals.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.IntStream;

class SignalBuilderTest {

	@Test
	void toBuilderRoundTrips() {
		Signal signal = Signal.builder().app("a").client("web").session("s").actor(Actor.agent("bot", "u")).action(Actions.SEARCH).target(Targets.INDEX, "i")
				.search(sd -> sd.query("q").resultCount(3)).duration(Duration.ofSeconds(2)).tag("k", "v").tag("n", 1).build();
		Assertions.assertEquals(signal, signal.toBuilder().build(), "same id, time, and fields");
		Signal stamped = signal.toBuilder().tag("host", "node-1").build();
		Assertions.assertEquals(signal.signalId(), stamped.signalId());
		Assertions.assertEquals("node-1", stamped.tags().get("host"));
	}

	@Test
	void searchDetailsAccumulateAcrossCalls() {
		Signal signal = Signal.builder().app("a").actor(Actor.user("u")).action(Actions.SEARCH).search(sd -> sd.query("x").indexes("idx"))
				.search(sd -> sd.searchId("saved-7")).build();
		Assertions.assertEquals("x", signal.search().query(), "first call's details kept");
		Assertions.assertEquals(List.of("idx"), signal.search().indexes());
		Assertions.assertEquals("saved-7", signal.search().searchId());
		Signal page = Signal.builder().app("a").actor(Actor.user("u")).action("page").searchId("saved-7").build();
		Assertions.assertEquals("saved-7", page.search().searchId(), "details created");
		Assertions.assertNull(Signal.builder().app("a").actor(Actor.user("u")).action("page").searchId(null).build().search(), "null key, no details");
		Signal stamped = Signal.builder().app("a").actor(Actor.user("u")).action(Actions.SEARCH).search(sd -> sd.query("x")).searchId("saved-7").build();
		Assertions.assertEquals("x", stamped.search().query(), "existing details kept");
		IllegalArgumentException blank = Assertions.assertThrows(IllegalArgumentException.class,
				() -> Signal.builder().app("a").actor(Actor.user("u")).action(Actions.SEARCH).searchId(" ").build());
		Assertions.assertTrue(blank.getMessage().contains("Search id"), blank.getMessage());
	}

	@Test
	void sessionHashedNeverStoresTheSecret() {
		String token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1LTEwNDIifQ.signature";
		Signal signal = Signal.builder().app("a").actor(Actor.user("u")).action(Actions.VIEW).sessionHashed(token).build();
		Assertions.assertEquals(32, signal.sessionId().length(), "16 bytes of SHA-256 as hex");
		Assertions.assertFalse(signal.sessionId().contains("eyJ"), "no part of the token survives");
		Assertions.assertEquals(signal.sessionId(), SessionIds.hashed(token), "stable for the same secret");
		Assertions.assertEquals("ba7816bf8f01cfea414140de5dae2223", SessionIds.hashed("abc"), "the first 16 bytes of SHA-256");
		Assertions.assertNotEquals(signal.sessionId(), SessionIds.hashed(token + "x"));
		Assertions.assertNull(SessionIds.hashed(null));
		Assertions.assertNull(SessionIds.hashed(" "));
		Assertions.assertNull(Signal.builder().app("a").actor(Actor.user("u")).action(Actions.VIEW).sessionHashed(null).build().sessionId(), "unset, not blank");
	}

	@Test
	void defaultsFillIdAndTimestamp() {
		Instant before = Instant.now();
		Signal signal = Signal.builder().app("search-app").actor(Actor.user("u1")).action(Actions.VIEW).build();
		Assertions.assertEquals(4, UUID.fromString(signal.signalId()).version(), "random uuid");
		Assertions.assertFalse(signal.timestamp().isBefore(before));
		Assertions.assertTrue(signal.tags().isEmpty());
		Assertions.assertNull(signal.target());
		Assertions.assertNull(signal.search());
	}

	@Test
	void fluentSearchSignal() {
		Signal signal = Signal.builder().app("search-app").client("web").session("s1").actor(Actor.user("u1")).action(Actions.SEARCH).target("index", "pubmed")
				.search(sd -> sd.query("lung cancer").indexes("pubmed").resultCount(412).latency(Duration.ofMillis(38))).tag("filterCount", "3").build();
		Assertions.assertEquals("lung cancer", signal.search().query());
		Assertions.assertEquals(412L, signal.search().resultCount());
		Assertions.assertEquals(38L, signal.search().latencyMs());
		Assertions.assertEquals(new Target("index", "pubmed"), signal.target());
		Assertions.assertEquals(Map.of("filterCount", "3"), signal.tags());
	}

	@Test
	void bulkTargetKeepsDistinctIdsInOrder() {
		Signal bulk = Signal.builder().app("a").actor(Actor.user("u")).action(Actions.ANNOTATE).target(Targets.RECORD, List.of("r2", "r1", "r2")).build();
		Assertions.assertEquals(new Target(Targets.RECORD, List.of("r2", "r1")), bulk.target());
		Signal one = Signal.builder().app("a").actor(Actor.user("u")).action(Actions.ANNOTATE).target(Targets.RECORD, List.of("r1")).build();
		Assertions.assertEquals(new Target(Targets.RECORD, "r1"), one.target(), "one id either way");

		IllegalArgumentException empty = Assertions.assertThrows(IllegalArgumentException.class, () -> new Target(Targets.RECORD, List.of()));
		Assertions.assertTrue(empty.getMessage().contains("empty"), empty.getMessage());
		IllegalArgumentException nullList = Assertions.assertThrows(IllegalArgumentException.class,
				() -> Signal.builder().target(Targets.RECORD, (Collection<String>) null).build());
		Assertions.assertTrue(nullList.getMessage().contains("null"), nullList.getMessage());
		IllegalArgumentException blank = Assertions.assertThrows(IllegalArgumentException.class,
				() -> Signal.builder().target(Targets.RECORD, Arrays.asList("r1", " ")).build());
		Assertions.assertTrue(blank.getMessage().contains("id 1 of 2") && blank.getMessage().contains("blank"), blank.getMessage());
		List<String> tooMany = IntStream.rangeClosed(0, Target.MAX_IDS).mapToObj(Integer::toString).toList();
		IllegalArgumentException cap = Assertions.assertThrows(IllegalArgumentException.class, () -> new Target(Targets.RECORD, tooMany));
		Assertions.assertTrue(cap.getMessage().contains(String.valueOf(Target.MAX_IDS)), cap.getMessage());
	}

	@Test
	void missingRequiredFieldsNameTheField() {
		IllegalArgumentException noApp = Assertions.assertThrows(IllegalArgumentException.class,
				() -> Signal.builder().actor(Actor.user("u1")).action(Actions.VIEW).build());
		Assertions.assertTrue(noApp.getMessage().startsWith("App is required"), noApp.getMessage());
		IllegalArgumentException noActor = Assertions.assertThrows(IllegalArgumentException.class,
				() -> Signal.builder().app("search-app").action(Actions.VIEW).build());
		Assertions.assertTrue(noActor.getMessage().startsWith("Actor is required"), noActor.getMessage());
		IllegalArgumentException blankAction = Assertions.assertThrows(IllegalArgumentException.class,
				() -> Signal.builder().app("search-app").actor(Actor.user("u1")).action(" ").build());
		Assertions.assertTrue(blankAction.getMessage().contains("Action is required") && blankAction.getMessage().contains("blank"), blankAction.getMessage());
	}

	@Test
	void tagsAreImmutableAndValidated() {
		Signal signal = Signal.builder().app("a").actor(Actor.system()).action(Actions.LOGIN).tag("k", "v").build();
		Assertions.assertThrows(UnsupportedOperationException.class, () -> signal.tags().put("x", "y"));
		IllegalArgumentException nullValue = Assertions.assertThrows(IllegalArgumentException.class, () -> Signal.builder().tag("k", (String) null).build());
		Assertions.assertTrue(nullValue.getMessage().contains("k"), nullValue.getMessage());
		IllegalArgumentException blankValue = Assertions.assertThrows(IllegalArgumentException.class, () -> Signal.builder().tag("k", " ").build());
		Assertions.assertTrue(blankValue.getMessage().contains("blank"), blankValue.getMessage());
		Signal typed = Signal.builder().app("a").actor(Actor.system()).action(Actions.LOGIN).tag("n", 5).tag("b", true).tag("e", ActorType.AGENT).build();
		Assertions.assertEquals(5L, typed.tags().get("n"));
		Assertions.assertEquals(true, typed.tags().get("b"));
		Assertions.assertEquals("AGENT", typed.tags().get("e"));
		Signal listed = Signal.builder().app("a").actor(Actor.system()).action(Actions.LOGIN).tag("l", List.of("x", "y")).build();
		Assertions.assertEquals(List.of("x", "y"), listed.tags().get("l"));
		Assertions.assertEquals(List.of(), Signal.builder().app("a").actor(Actor.system()).action(Actions.LOGIN).tag("l", List.of()).build().tags().get("l"),
				"an empty list is a value");
		Assertions.assertThrows(IllegalArgumentException.class, () -> Signal.builder().tag("l", (Collection<String>) null).build(), "null list");
		Signal optional = Signal.builder().app("a").actor(Actor.user("secret-id")).action(Actions.LOGIN).session(" ").client("").tagIfPresent("o", " ")
				.tagIfPresent("e", (Enum<?>) null).tagIfPresent("p", "x").build();
		Assertions.assertNull(optional.sessionId(), "blank session is unset");
		Assertions.assertNull(optional.client(), "blank client is unset");
		Assertions.assertEquals(Map.of("p", "x"), optional.tags(), "tagIfPresent skips blank and null");
		Assertions.assertFalse(optional.toString().contains("secret-id"), "toString hides the actor id");
		IllegalArgumentException dotted = Assertions.assertThrows(IllegalArgumentException.class, () -> Signal.builder().tag("a.b", "v").build());
		Assertions.assertTrue(dotted.getMessage().contains("a.b"), dotted.getMessage());
	}

	@Test
	void shownDocIdsAreCappedAndCopied() {
		java.util.List<String> tooMany = java.util.stream.IntStream.range(0, SearchDetails.MAX_SHOWN_DOC_IDS + 1).mapToObj(i -> "d" + i).toList();
		IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class, () -> SearchDetails.builder().shownDocIds(tooMany).build());
		Assertions.assertTrue(error.getMessage().contains(String.valueOf(SearchDetails.MAX_SHOWN_DOC_IDS + 1)), error.getMessage());
		Assertions.assertEquals(java.util.List.of(), SearchDetails.builder().query("q").build().shownDocIds(), "absent list reads as empty");
	}

	@Test
	void durationIsOptionalAndNeverNegative() {
		Signal logout = Signal.builder().app("search-app").actor(Actor.user("u1")).action(Actions.LOGOUT).duration(Duration.ofMinutes(2)).build();
		Assertions.assertEquals(120_000L, logout.durationMs());
		Assertions.assertNull(Signal.builder().app("search-app").actor(Actor.user("u1")).action(Actions.VIEW).build().durationMs());
		IllegalArgumentException negative = Assertions.assertThrows(IllegalArgumentException.class,
				() -> Signal.builder().duration(Duration.ofSeconds(-1)).build());
		Assertions.assertTrue(negative.getMessage().contains("negative"), negative.getMessage());
		Assertions.assertThrows(IllegalArgumentException.class, () -> Signal.builder().duration(null).build());
	}

	@Test
	void setterFailuresWaitForBuild() {
		Signal.Builder builder = Signal.builder().app("search-app").actor(Actor.user("u1")).action(Actions.UPDATE);
		Assertions.assertSame(builder, builder.target("template", (String) null), "a bad value never throws from the setter");
		Assertions.assertSame(builder, builder.tag("k", " ").duration(Duration.ofSeconds(-1)), "later setters still chain");
		IllegalArgumentException all = Assertions.assertThrows(IllegalArgumentException.class, builder::build);
		Assertions.assertTrue(all.getMessage().startsWith("3 setter failures. Target id is required for target type template but was null. Tag k must have"),
				all.getMessage());
		Assertions.assertTrue(all.getMessage().endsWith("Duration must not be negative but was PT-1S"), all.getMessage());
		Signal repaired = builder.target("template", "t1").tag("k", "v").duration(Duration.ZERO).build();
		Assertions.assertEquals(new Target("template", "t1"), repaired.target(), "a later call on the same setter clears its failure");
		Assertions.assertEquals(Map.of("k", "v"), repaired.tags());
		Assertions.assertEquals(0L, repaired.durationMs());
		IllegalArgumentException beforeRequired = Assertions.assertThrows(IllegalArgumentException.class, () -> Signal.builder().tag("k", " ").build());
		Assertions.assertTrue(beforeRequired.getMessage().startsWith("Tag k"), "the setter failure wins over the missing app: " + beforeRequired.getMessage());
		IllegalArgumentException otherKey = Assertions.assertThrows(IllegalArgumentException.class,
				() -> Signal.builder().app("a").actor(Actor.user("u1")).action(Actions.VIEW).tag("k", (String) null).tag("j", "fixed").build());
		Assertions.assertTrue(otherKey.getMessage().startsWith("Tag k must have a value but was null"),
				"a single failure is thrown as it was, another key does not clear it");
		Signal.Builder npe = Signal.builder().app("a").actor(Actor.user("u1")).action(Actions.SEARCH);
		Assertions.assertSame(npe, npe.search(sd -> sd.shownDocIds(Arrays.asList("d1", null))), "a null element fails inside the consumer");
		IllegalArgumentException consumer = Assertions.assertThrows(IllegalArgumentException.class, npe::build,
				"caller code failing in a setter waits for build");
		Assertions.assertTrue(consumer.getMessage().startsWith("Search details failed, "), consumer.getMessage());
	}

	@Test
	void readyMadeValuesClearTheirSetterProblem() {
		Signal.Builder builder = Signal.builder().app("a").actor(Actor.user("u1")).action(Actions.SEARCH);
		builder.target("template", (String) null).search(sd -> sd.searchId(" "));
		Assertions.assertThrows(IllegalArgumentException.class, builder::build);
		Signal signal = builder.target(new Target("template", "t1")).search(SearchDetails.builder().searchId("s1").build()).build();
		Assertions.assertEquals(new Target("template", "t1"), signal.target());
		Assertions.assertEquals("s1", signal.search().searchId());
	}

	@Test
	void extremeValuesAreProblemsNotExceptions() {
		Signal.Builder builder = Signal.builder().app("a").actor(Actor.user("u1")).action(Actions.VIEW);
		Assertions.assertSame(builder,
				builder.duration(Duration.ofSeconds(Long.MAX_VALUE)).tag("when", Instant.MAX).search((Consumer<SearchDetails.Builder>) null));
		IllegalArgumentException all = Assertions.assertThrows(IllegalArgumentException.class, builder::build);
		Assertions.assertTrue(all.getMessage().contains("does not fit milliseconds in a long"), all.getMessage());
		Assertions.assertTrue(all.getMessage().contains("Tag when instant +1000000000-12-31T23:59:59.999999999Z does not fit a Date"), all.getMessage());
		Assertions.assertTrue(all.getMessage().contains("Search details failed, "), "a null consumer is caught like any consumer failure: " + all.getMessage());
		Signal signal = builder.duration(Duration.ofMillis(Long.MAX_VALUE)).tag("when", Instant.EPOCH).search(sd -> sd.searchId("s1")).build();
		Assertions.assertEquals(Long.MAX_VALUE, signal.durationMs());
		Assertions.assertEquals(Date.from(Instant.EPOCH), signal.tags().get("when"));
	}

	@Test
	void noPositionalConstructorIsReachable() {
		for (Constructor<?> constructor : Signal.class.getDeclaredConstructors()) {
			Assertions.assertTrue(Modifier.isPrivate(constructor.getModifiers()), "constructor must be private: " + constructor);
		}
	}
}

package io.zulia.signals.storage;

import io.zulia.signals.model.Actions;
import io.zulia.signals.model.Actor;
import io.zulia.signals.model.Signal;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;

class SignalEnricherTest {

	private static final Instant EVENT_TIME = Instant.parse("2026-08-27T15:00:00Z");
	private static final Instant RECEIVED = Instant.parse("2026-08-27T15:00:05Z");
	private static final Clock CLOCK = Clock.fixed(RECEIVED, ZoneOffset.UTC);

	@Test
	void stampsBucketsAndReceivedAt() {
		Signal signal = Signal.builder().app("search-app").timestamp(EVENT_TIME).actor(Actor.user("u1")).action(Actions.VIEW).build();
		Document doc = new SignalEnricher(CLOCK, ZoneOffset.UTC, ActorIdMapper.identity()).toDocument(signal);
		Assertions.assertEquals("2026-08-27", doc.getString(SignalField.DAY.fieldName()));
		Assertions.assertEquals("2026-W35", doc.getString(SignalField.WEEK.fieldName()));
		Assertions.assertEquals("2026-08", doc.getString(SignalField.MONTH.fieldName()));
		Assertions.assertEquals(Date.from(RECEIVED), doc.get(SignalField.RECEIVED_AT.fieldName(), Date.class));
		Assertions.assertEquals("USER", doc.getString(SignalField.ACTOR_TYPE.fieldName()));
		Assertions.assertEquals("u1", doc.getString(SignalField.ACTOR_ID.fieldName()));
		Assertions.assertFalse(doc.containsKey(SignalField.TARGET_TYPE.fieldName()), "absent target leaves no field");
		Assertions.assertFalse(doc.containsKey(SignalField.TAGS), "no tags leaves no field");
	}

	@Test
	void flattensSearchAndTags() {
		Signal signal = Signal.builder().app("search-app").timestamp(EVENT_TIME).actor(Actor.agent("zulia-agent", "u1")).action(Actions.CLICK)
				.target("document", "PMC1").search(sd -> sd.query("lung cancer").clickedPosition(2).clickedDocId("PMC1")).tag("panel", "left").build();
		Document doc = new SignalEnricher(CLOCK, ZoneOffset.UTC, ActorIdMapper.identity()).toDocument(signal);
		Assertions.assertEquals("u1", doc.getString(SignalField.DELEGATED_BY.fieldName()));
		Assertions.assertEquals("lung cancer", doc.getString(SignalField.SEARCH_QUERY.fieldName()));
		Assertions.assertEquals(2, doc.getInteger(SignalField.SEARCH_CLICKED_POSITION.fieldName()));
		Assertions.assertEquals("document", doc.getString(SignalField.TARGET_TYPE.fieldName()));
		Assertions.assertEquals("PMC1", doc.getString(SignalField.TARGET_ID.fieldName()), "one id stays a scalar");
		Assertions.assertEquals("left", doc.get(SignalField.TAGS, Document.class).getString("panel"));
	}

	@Test
	void bulkTargetStoresTheIdList() {
		Signal bulk = Signal.builder().app("curation-app").timestamp(EVENT_TIME).actor(Actor.user("u1")).action(Actions.ANNOTATE)
				.target("record", List.of("r1", "r2")).build();
		Document doc = new SignalEnricher(CLOCK, ZoneOffset.UTC, ActorIdMapper.identity()).toDocument(bulk);
		Assertions.assertEquals("record", doc.getString(SignalField.TARGET_TYPE.fieldName()));
		Assertions.assertEquals(List.of("r1", "r2"), doc.getList(SignalField.TARGET_ID.fieldName(), String.class), "one facet value per id");
	}

	@Test
	void searchSignalsCarryLinkageImpressionsAndNormalizedQuery() {
		Signal search = Signal.builder().app("search-app").timestamp(EVENT_TIME).actor(Actor.user("u1")).action(Actions.SEARCH)
				.search(sd -> sd.query("  Lung   CANCER ").indexes("pubmed", "pmc").resultCount(2).searchId("saved-7").shownDocIds(List.of("PMC1", "PMC2")))
				.build();
		Document searchDoc = new SignalEnricher(CLOCK, ZoneOffset.UTC, ActorIdMapper.identity()).toDocument(search);
		Assertions.assertEquals("lung cancer", searchDoc.getString(SignalField.SEARCH_QUERY_NORMALIZED.fieldName()));
		Assertions.assertEquals("  Lung   CANCER ", searchDoc.getString(SignalField.SEARCH_QUERY.fieldName()), "raw query kept");
		Assertions.assertEquals(List.of("PMC1", "PMC2"), searchDoc.getList(SignalField.SEARCH_SHOWN_DOC_IDS.fieldName(), String.class));
		Assertions.assertEquals(List.of("pubmed", "pmc"), searchDoc.getList(SignalField.SEARCH_INDEX.fieldName(), String.class), "one facet value per index");
		Assertions.assertEquals("saved-7", searchDoc.getString(SignalField.SEARCH_ID.fieldName()));

		Signal click = Signal.builder().app("search-app").timestamp(EVENT_TIME).actor(Actor.user("u1")).action(Actions.CLICK)
				.search(sd -> sd.searchSignalId(search.signalId()).clickedPosition(1).clickedDocId("PMC2")).build();
		Document clickDoc = new SignalEnricher(CLOCK, ZoneOffset.UTC, ActorIdMapper.identity()).toDocument(click);
		Assertions.assertEquals(search.signalId(), clickDoc.getString(SignalField.SEARCH_SIGNAL_ID.fieldName()));
		Assertions.assertFalse(clickDoc.containsKey(SignalField.SEARCH_SHOWN_DOC_IDS.fieldName()), "clicks carry no impressions");
		Assertions.assertFalse(clickDoc.containsKey(SignalField.SEARCH_QUERY_NORMALIZED.fieldName()), "no normalized key without a query");
	}

	@Test
	void storesTagsInDeclaredKindsAndDuration() {
		SignalsIndexConfig config = SignalsIndexConfig.defaults().indexTags("division", "dynamic").indexTag("records", SignalField.Kind.LONG);
		Signal signal = Signal.builder().app("curation-app").timestamp(EVENT_TIME).actor(Actor.user("u1")).action(Actions.LOGOUT)
				.duration(Duration.ofMinutes(2)).tag("division", "north").tag("dynamic", true).tag("records", 12).tag("panel", "left")
				.tag("fields", List.of("title", "abstract")).build();
		Document doc = new SignalEnricher(config, CLOCK).toDocument(signal);
		Document tags = doc.get(SignalField.TAGS, Document.class);
		Assertions.assertEquals("north", tags.getString("division"));
		Assertions.assertEquals("true", tags.getString("dynamic"), "keyword tag stores the string form");
		Assertions.assertEquals(12L, tags.getLong("records"), "numeric tag stores the number");
		Assertions.assertEquals("left", tags.getString("panel"), "undeclared tags are stored as tagged");
		Assertions.assertEquals(List.of("title", "abstract"), tags.getList("fields", String.class), "list tags stay lists");
		Assertions.assertFalse(doc.containsKey("division"), "nothing at the top level");
		Assertions.assertEquals(120_000L, doc.getLong(SignalField.DURATION_MS.fieldName()));
		Signal mismatch = Signal.builder().app("curation-app").timestamp(EVENT_TIME).actor(Actor.user("u1")).action(Actions.LOGOUT).tag("records", "many")
				.build();
		IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class, () -> new SignalEnricher(config, CLOCK).toDocument(mismatch));
		Assertions.assertTrue(error.getMessage().contains("records") && error.getMessage().contains("LONG"), error.getMessage());
	}

	@Test
	void hmacMapperPseudonymizesBothIds() {
		ActorIdMapper mapper = ActorIdMapper.hmacSha256("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8));
		Signal signal = Signal.builder().app("search-app").timestamp(EVENT_TIME).actor(Actor.agent("zulia-agent", "u1")).action(Actions.VIEW).build();
		Document doc = new SignalEnricher(CLOCK, ZoneOffset.UTC, mapper).toDocument(signal);
		String actorId = doc.getString(SignalField.ACTOR_ID.fieldName());
		Assertions.assertEquals(64, actorId.length());
		Assertions.assertNotEquals("zulia-agent", actorId);
		Assertions.assertEquals(mapper.map("search-app", "u1"), doc.getString(SignalField.DELEGATED_BY.fieldName()));
		Assertions.assertNotEquals(mapper.map("curation-app", "u1"), doc.getString(SignalField.DELEGATED_BY.fieldName()), "pseudonyms are per app");
		Assertions.assertEquals(actorId, new SignalEnricher(CLOCK, ZoneOffset.UTC, mapper).storedActorId("search-app", "zulia-agent"),
				"reports filter on the same pseudonym");
	}

	@Test
	void configWithMapperKeepsZoneAndIndexedTags() {
		ActorIdMapper mapper = ActorIdMapper.hmacSha256("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8));
		SignalsIndexConfig config = SignalsIndexConfig.defaults().zone(ZoneId.of("America/New_York")).indexTags("division");
		Signal signal = Signal.builder().app("curation-app").timestamp(Instant.parse("2026-09-01T02:00:00Z")).actor(Actor.user("u1")).action(Actions.VIEW)
				.tag("division", "north").build();
		Document doc = new SignalEnricher(config, CLOCK, mapper).toDocument(signal);
		Assertions.assertEquals(mapper.map("curation-app", "u1"), doc.getString(SignalField.ACTOR_ID.fieldName()));
		Assertions.assertEquals("north", doc.get(SignalField.TAGS, Document.class).getString("division"), "indexed tags from config");
		Assertions.assertEquals("2026-08-31", doc.getString(SignalField.DAY.fieldName()), "buckets in the config zone");
	}

	@Test
	void weekBucketFollowsIsoYearAtTheBoundary() {
		Assertions.assertEquals("2026-W53", SignalEnricher.week(java.time.LocalDate.of(2027, 1, 1)));
		Assertions.assertEquals("2027-W01", SignalEnricher.week(java.time.LocalDate.of(2027, 1, 4)));
	}

	@Test
	void typedTagsStoreDatesListsAndRejectOverflow() {
		SignalsIndexConfig config = SignalsIndexConfig.defaults().indexTags("labels").indexTag("uploadedAt", SignalField.Kind.DATE)
				.indexTag("rows", SignalField.Kind.INT);
		Instant uploadedAt = Instant.parse("2026-09-01T02:00:00Z");
		Signal signal = Signal.builder().app("curation-app").timestamp(EVENT_TIME).actor(Actor.user("u1")).action(Actions.UPLOAD)
				.tag("labels", List.of("a", "b")).tag("uploadedAt", uploadedAt).tag("rows", 42).build();
		Document tags = new SignalEnricher(config, CLOCK).toDocument(signal).get(SignalField.TAGS, Document.class);
		Assertions.assertEquals(List.of("a", "b"), tags.getList("labels", String.class), "a list on a keyword tag stays a list");
		Assertions.assertEquals(Date.from(uploadedAt), tags.getDate("uploadedAt"));
		Assertions.assertEquals(42, tags.getInteger("rows"));
		Signal tooBig = Signal.builder().app("curation-app").timestamp(EVENT_TIME).actor(Actor.user("u1")).action(Actions.UPLOAD).tag("rows", 5_000_000_000L)
				.build();
		IllegalArgumentException overflow = Assertions.assertThrows(IllegalArgumentException.class, () -> new SignalEnricher(config, CLOCK).toDocument(tooBig));
		Assertions.assertTrue(overflow.getMessage().contains("rows") && overflow.getMessage().contains("INT"), overflow.getMessage());
		Signal notADate = Signal.builder().app("curation-app").timestamp(EVENT_TIME).actor(Actor.user("u1")).action(Actions.UPLOAD).tag("uploadedAt", "today")
				.build();
		IllegalArgumentException mismatch = Assertions.assertThrows(IllegalArgumentException.class,
				() -> new SignalEnricher(config, CLOCK).toDocument(notADate));
		Assertions.assertTrue(mismatch.getMessage().contains("DATE"), mismatch.getMessage());
	}
}

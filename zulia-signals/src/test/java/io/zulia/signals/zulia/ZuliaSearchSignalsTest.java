package io.zulia.signals.zulia;

import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.command.builder.TermQuery;
import io.zulia.client.result.SearchResult;
import io.zulia.message.ZuliaQuery.ScoredResult;
import io.zulia.message.ZuliaServiceOuterClass.QueryResponse;
import io.zulia.signals.model.Actions;
import io.zulia.signals.model.Actor;
import io.zulia.signals.model.Signal;
import io.zulia.signals.model.Targets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

class ZuliaSearchSignalsTest {

	private static SearchResult resultWith(long totalHits, String... ids) {
		QueryResponse.Builder response = QueryResponse.newBuilder().setTotalHits(totalHits);
		for (String id : ids) {
			response.addResults(ScoredResult.newBuilder().setUniqueId(id).setIndexName("pubmed"));
		}
		return new SearchResult(response.build());
	}

	@Test
	void complicatedSearchMapsToOneSignal() {
		Search search = new Search("pubmed", "pmc").setAmount(25).setStart(50).setSearchLabel("main-search");
		search.addQuery(new ScoredQuery("lung cancer").addQueryFields("title", "abstract"));
		search.addQuery(new ScoredQuery("immunotherapy").addQueryField("abstract"));
		search.addQuery(new FilterQuery("year:[2020 TO 2026]"));
		search.addQuery(new FilterQuery("type:review").exclude());
		search.addQuery(new TermQuery("journal").addTerms("Nature", "Science"));
		search.addSort(new Sort("year").descending());
		search.addCountFacet(new CountFacet("journal"));
		SearchResult result = resultWith(412, "PMC1", "PMC2", "PMC3");

		Signal signal = ZuliaSearchSignals.searchSignal(search, result, Duration.ofMillis(38)).app("search-app").actor(Actor.user("u1")).searchId("saved-7")
				.build();

		Assertions.assertEquals(Actions.SEARCH, signal.actionType());
		Assertions.assertEquals(List.of("pubmed", "pmc"), signal.target().ids(), "one target per index");
		Assertions.assertEquals("lung cancer immunotherapy", signal.search().query(), "scored clauses only, in order");
		Assertions.assertEquals("saved-7", signal.search().searchId(), "mapped details kept");
		Assertions.assertEquals(List.of("pubmed", "pmc"), signal.search().indexes(), "indexes stay a list");
		Assertions.assertEquals(412L, signal.search().resultCount());
		Assertions.assertEquals(38L, signal.search().latencyMs());
		Assertions.assertEquals(List.of("PMC1", "PMC2", "PMC3"), signal.search().shownDocIds());
		Assertions.assertEquals(50L, signal.tags().get("start"));
		Assertions.assertEquals(25L, signal.tags().get("amount"));
		Assertions.assertEquals(3L, signal.tags().get("filterClauses"), "term list counts as a filter");
		Assertions.assertEquals(List.of("title", "abstract"), signal.tags().get("queryFields"), "scored clause fields only");
		Assertions.assertEquals(List.of("year"), signal.tags().get("sort"));
		Assertions.assertEquals(1L, signal.tags().get("facets"));
		Assertions.assertEquals("main-search", signal.tags().get("searchLabel"));
		Assertions.assertNull(signal.tags().get("vectorClauses"), "absent counts are not written");
	}

	@Test
	void latencyDefaultsToTheClientRoundTrip() {
		Search search = new Search("pubmed").setAmount(10);
		search.addQuery(new ScoredQuery("x"));
		SearchResult result = resultWith(1, "PMC1");
		result.setCommandTimeMs(57);
		Signal signal = ZuliaSearchSignals.searchSignal(search, result).app("a").actor(Actor.user("u")).build();
		Assertions.assertEquals(57L, signal.search().latencyMs());
	}

	@Test
	void impressionsAreCapped() {
		String[] ids = new String[30];
		for (int i = 0; i < ids.length; i++) {
			ids[i] = "d" + i;
		}
		Search search = new Search("pubmed").setAmount(30);
		search.addQuery(new ScoredQuery("x"));
		Signal signal = ZuliaSearchSignals.searchSignal(search, resultWith(30, ids), Duration.ZERO).app("a").actor(Actor.user("u")).build();
		Assertions.assertEquals(20, signal.search().shownDocIds().size());
	}

	@Test
	void clickLinksBackAndCarriesTheActor() {
		Search search = new Search("pubmed").setAmount(10);
		search.addQuery(new ScoredQuery("lung cancer"));
		Signal searchSignal = ZuliaSearchSignals.searchSignal(search, resultWith(2, "PMC1", "PMC2"), Duration.ofMillis(5)).app("search-app").client("web")
				.session("s1").actor(Actor.agent("zulia-agent", "u1")).searchId("saved-7").build();

		Signal click = ZuliaSearchSignals.click(searchSignal, "PMC2", 1).build();

		Assertions.assertEquals(Actions.CLICK, click.actionType());
		Assertions.assertEquals(searchSignal.signalId(), click.search().searchSignalId());
		Assertions.assertNull(click.search().query(), "follow ups carry no query text");
		Assertions.assertTrue(click.search().indexes().isEmpty());
		Assertions.assertEquals("saved-7", click.search().searchId(), "searchId carried");
		Assertions.assertEquals(1, click.search().clickedPosition());
		Assertions.assertEquals("PMC2", click.search().clickedDocId());
		Assertions.assertEquals(searchSignal.actor(), click.actor());
		Assertions.assertEquals("s1", click.sessionId());
		Assertions.assertEquals("web", click.client());
		Assertions.assertTrue(click.search().shownDocIds().isEmpty(), "no impressions on a click");

		Signal export = ZuliaSearchSignals.linked(searchSignal, Actions.EXPORT, "PMC1", 0).build();
		Assertions.assertEquals(Actions.EXPORT, export.actionType());
		Assertions.assertEquals(searchSignal.signalId(), export.search().searchSignalId());
		Assertions.assertEquals(Targets.DOCUMENT, export.target().type(), "default follow-up target type");

		Signal saved = ZuliaSearchSignals.linked(searchSignal, Actions.SAVE, Targets.RECORD, "PMC1", 0).build();
		Assertions.assertEquals(Targets.RECORD, saved.target().type(), "caller chosen target type");
		Assertions.assertEquals(searchSignal.signalId(), saved.search().searchSignalId());
	}

	@Test
	void linkingToANonSearchSignalIsRejected() {
		Signal view = Signal.builder().app("a").actor(Actor.user("u")).action(Actions.VIEW).build();
		IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class, () -> ZuliaSearchSignals.click(view, "d", 0));
		Assertions.assertTrue(error.getMessage().contains(view.signalId()) && error.getMessage().contains(Actions.VIEW), error.getMessage());
	}

}

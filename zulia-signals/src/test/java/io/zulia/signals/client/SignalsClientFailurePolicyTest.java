package io.zulia.signals.client;

import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.message.ZuliaQuery.ScoredResult;
import io.zulia.message.ZuliaServiceOuterClass.QueryResponse;
import io.zulia.signals.model.Actions;
import io.zulia.signals.model.Actor;
import io.zulia.signals.model.Signal;
import io.zulia.signals.storage.SignalField;
import io.zulia.signals.storage.SignalsIndexConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

class SignalsClientFailurePolicyTest {

	// Port 1 refuses connections immediately, and zero retries keeps the refusal from turning into a wait.
	private static ZuliaWorkPool unreachablePool;

	@BeforeAll
	static void unreachablePool() throws Exception {
		unreachablePool = new ZuliaWorkPool(new ZuliaPoolConfig().addNode("localhost", 1).setDefaultRetries(0).setNodeUpdateEnabled(false));
	}

	@AfterAll
	static void shutdown() throws Exception {
		unreachablePool.shutdown();
	}

	private static void awaitDrops(SignalsClient client, long expected) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
		while (client.droppedSignals() < expected && System.nanoTime() < deadline) {
			Thread.sleep(20);
		}
		Assertions.assertEquals(expected, client.droppedSignals(), "drops are counted by the writer thread");
	}

	private static Signal signal() {
		return Signal.builder().app("search-app").actor(Actor.user("u1")).action(Actions.VIEW).build();
	}

	@Test
	void propagateThrowsByDefault() {
		SignalsClient client = new SignalsClient(unreachablePool, SignalsIndexConfig.defaults());
		Assertions.assertEquals(RecordFailurePolicy.PROPAGATE, client.failurePolicy());
		SignalsException stored = Assertions.assertThrows(SignalsException.class, () -> client.record(signal()));
		Assertions.assertNotNull(stored.getCause(), "the pool failure is the cause");
		Assertions.assertThrows(SignalsException.class, client::ensureStorage);
	}

	@Test
	void logAndDropKeepsTheCallerRunning() throws Exception {
		SignalsClient client = new SignalsClient(unreachablePool, SignalsIndexConfig.defaults()).onFailure(RecordFailurePolicy.LOG_AND_DROP);
		client.ensureStorage();
		RecordResult result = client.record(signal());
		Assertions.assertTrue(result.accepted(), "queued for the writer, the caller never waits");
		List<RecordResult> results = client.recordAll(List.of(signal(), signal()));
		Assertions.assertEquals(2, results.size());
		Assertions.assertTrue(results.stream().allMatch(RecordResult::accepted));
		awaitDrops(client, 3);
		client.close();
		Assertions.assertEquals(0, client.queuedSignals(), "close drains the queue");
	}

	@Test
	void logAndDropNeverThrowsForABadSignal() throws Exception {
		SignalsIndexConfig typed = SignalsIndexConfig.defaults().indexTag("rows", SignalField.Kind.LONG);
		SignalsClient client = new SignalsClient(unreachablePool, typed).onFailure(RecordFailurePolicy.LOG_AND_DROP);
		RecordResult unbuilt = client.record(Signal.builder().actor(Actor.user("u1")).action(Actions.VIEW));
		Assertions.assertFalse(unbuilt.accepted(), "no app, so it never built");
		Assertions.assertNull(unbuilt.signalId());
		Signal wrongKind = Signal.builder().app("search-app").actor(Actor.user("u1")).action(Actions.VIEW).tag("rows", "many").build();
		RecordResult unenriched = client.record(wrongKind);
		Assertions.assertFalse(unenriched.accepted(), "a string on a LONG tag is rejected by the schema");
		Assertions.assertEquals(wrongKind.signalId(), unenriched.signalId());
		RecordResult unstamped = client.stamping(builder -> builder.tag("host", " ")).record(signal());
		Assertions.assertFalse(unstamped.accepted(), "a stamp that breaks the signal is a drop too");
		Assertions.assertEquals(3, client.droppedSignals(), "counted before the writer is involved");
		Assertions.assertEquals(0, client.queuedSignals());
		Assertions.assertThrows(IllegalArgumentException.class, () -> client.record((Signal.Builder) null));
		Assertions.assertThrows(IllegalArgumentException.class, () -> client.record((Signal) null), "a null signal is a caller bug under any policy");
		client.close();

		SignalsClient propagating = new SignalsClient(unreachablePool, typed);
		Assertions.assertThrows(IllegalArgumentException.class, () -> propagating.record(Signal.builder().actor(Actor.user("u1")).action(Actions.VIEW)),
				"PROPAGATE throws the validation error itself");
		Assertions.assertThrows(IllegalArgumentException.class, () -> propagating.record(wrongKind));
	}

	@Test
	void onFailureRequiresAPolicy() {
		SignalsClient client = new SignalsClient(unreachablePool, SignalsIndexConfig.defaults());
		IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class, () -> client.onFailure(null));
		Assertions.assertTrue(error.getMessage().contains("LOG_AND_DROP"), error.getMessage());
	}

	@Test
	void dropLogIsThrottledToOneLinePerInterval() {
		Instant start = Instant.parse("2026-08-29T10:00:00Z");
		MutableClock clock = new MutableClock(start);
		DroppedSignalLog log = new DroppedSignalLog(clock);
		Exception cause = new IllegalStateException("connection refused");
		log.dropped(signal(), cause);
		log.dropped(signal(), cause);
		log.dropped(signal(), cause);
		Assertions.assertEquals(1, log.reportsEmitted(), "one log per interval");
		clock.advance(DroppedSignalLog.REPORT_INTERVAL);
		log.dropped(signal(), cause);
		Assertions.assertEquals(2, log.reportsEmitted(), "summary at the boundary");
		Assertions.assertEquals(4, log.droppedTotal());
	}

	private static final class MutableClock extends Clock {

		private Instant now;

		MutableClock(Instant now) {
			this.now = now;
		}

		void advance(Duration duration) {
			now = now.plus(duration);
		}

		@Override
		public ZoneOffset getZone() {
			return ZoneOffset.UTC;
		}

		@Override
		public Clock withZone(java.time.ZoneId zone) {
			return this;
		}

		@Override
		public Instant instant() {
			return now;
		}
	}

	@Test
	void searchAndDeleteAlwaysThrow() {
		SignalsIndexConfig partitioned = SignalsIndexConfig.defaults().monthlyPartitions();
		SignalsClient client = new SignalsClient(unreachablePool, partitioned).onFailure(RecordFailurePolicy.LOG_AND_DROP);
		Assertions.assertThrows(Exception.class, () -> client.search(search -> search.setAmount(1)));
		SearchResult page = new SearchResult(
				QueryResponse.newBuilder().addResults(ScoredResult.newBuilder().setUniqueId("s1").setIndexName("signals-2026-01")).build());
		Assertions.assertThrows(Exception.class, () -> client.deleteSignals(page));
		Assertions.assertEquals(0, client.droppedSignals(), "not drops");
		IllegalArgumentException notAPartition = Assertions.assertThrows(IllegalArgumentException.class, () -> client.dropPartition("other-2026-01"));
		Assertions.assertTrue(notAPartition.getMessage().contains("other-2026-01"), notAPartition.getMessage());
		Exception unreachable = Assertions.assertThrows(Exception.class, () -> client.dropPartition("signals-2026-01"));
		Assertions.assertFalse(unreachable instanceof IllegalArgumentException, "a real partition name reaches the pool");
		Assertions.assertThrows(IllegalStateException.class,
				() -> new SignalsClient(unreachablePool, SignalsIndexConfig.defaults()).dropPartition("signals-2026-01"), "not partitioned");
	}
}

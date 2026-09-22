package io.zulia.signals.client;

import io.zulia.client.command.BatchDelete;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Search;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.message.ZuliaIndex.IndexAlias;
import io.zulia.signals.model.Signal;
import io.zulia.signals.storage.ActorIdMapper;
import io.zulia.signals.storage.SignalEnricher;
import io.zulia.signals.storage.SignalsIndexConfig;
import io.zulia.util.IndexAliasUtil;
import org.bson.Document;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * The failure policy covers record, recordAll, and ensureStorage. Everything else always throws. Record creates storage on first use,
 * moves the alias to the current month when it changes, and retries a failed setup once a minute, so ensureStorage is only for failing
 * fast at startup. Under LOG_AND_DROP record queues the signal for one writer thread and returns, so a request path never waits on
 * the pool. Close drains that queue.
 */
public final class SignalsClient implements AutoCloseable {

	static final Duration SETUP_RETRY_INTERVAL = Duration.ofMinutes(1);

	private final ZuliaWorkPool pool;
	private final SignalsIndexConfig config;
	private final SignalEnricher enricher;
	private final Clock clock;
	private final RecordFailurePolicy failurePolicy;
	private final DroppedSignalLog droppedSignalLog;
	private final StorageState storage;
	private final SignalWriter writer;
	private final Consumer<Signal.Builder> stamp;

	public SignalsClient(ZuliaWorkPool pool, SignalsIndexConfig config) {
		this(pool, config, ActorIdMapper.identity());
	}

	public SignalsClient(ZuliaWorkPool pool, SignalsIndexConfig config, ActorIdMapper actorIdMapper) {
		this(pool, config, actorIdMapper, Clock.systemUTC());
	}

	/** Builds the enricher from the config so zone and dimensions match the index schema. */
	public SignalsClient(ZuliaWorkPool pool, SignalsIndexConfig config, ActorIdMapper actorIdMapper, Clock clock) {
		this(pool, config, new SignalEnricher(config, clock, actorIdMapper), clock, RecordFailurePolicy.PROPAGATE, new DroppedSignalLog(clock),
				new StorageState(), null, null);
	}

	private SignalsClient(ZuliaWorkPool pool, SignalsIndexConfig config, SignalEnricher enricher, Clock clock, RecordFailurePolicy failurePolicy,
			DroppedSignalLog droppedSignalLog, StorageState storage, SignalWriter writer, Consumer<Signal.Builder> stamp) {
		this.pool = pool;
		this.config = config;
		this.enricher = enricher;
		this.clock = clock;
		this.failurePolicy = failurePolicy;
		this.droppedSignalLog = droppedSignalLog;
		this.storage = storage;
		this.writer = writer == null ? new SignalWriter(this::write) : writer;
		this.stamp = stamp;
	}

	/** Shares the drop log and the storage state. */
	public SignalsClient onFailure(RecordFailurePolicy failurePolicy) {
		if (failurePolicy == null) {
			throw new IllegalArgumentException("Failure policy is required, one of " + Arrays.toString(RecordFailurePolicy.values()));
		}
		return new SignalsClient(pool, config, enricher, clock, failurePolicy, droppedSignalLog, storage, writer, stamp);
	}

	/** Same client applying the stamp to every signal before it is stored, for a version or host tag. The stamp runs last, so it wins. */
	public SignalsClient stamping(Consumer<Signal.Builder> stamp) {
		if (stamp == null) {
			throw new IllegalArgumentException("Stamp is required, use the client as is to stamp nothing");
		}
		return new SignalsClient(pool, config, enricher, clock, failurePolicy, droppedSignalLog, storage, writer, stamp);
	}

	public RecordFailurePolicy failurePolicy() {
		return failurePolicy;
	}

	/** Creates the index, or the current month's partition and the alias. Idempotent. */
	public void ensureStorage() {
		try {
			createStorage();
		}
		catch (Exception e) {
			if (failurePolicy == RecordFailurePolicy.PROPAGATE) {
				throw new SignalsException("Creating storage " + config.readName() + " failed", e);
			}
			droppedSignalLog.setupFailed(config.readName(), e);
		}
	}

	/**
	 * Same as ensureStorage but always throws. The server's createIndex is a no-op when the settings match and applies schema changes
	 * otherwise, so a dimension declared later reaches the index without a separate update.
	 */
	void createStorage() throws Exception {
		if (!config.isPartitioned()) {
			pool.createIndex(config.clientIndexConfig(config.indexName()));
			storage.setReadyIndex(config.indexName());
			return;
		}
		String currentPartition = currentPartition();
		pool.createIndex(config.clientIndexConfig(currentPartition));
		try {
			List<String> indexNames = mergeIndexes(existingPartitions(), currentPartition);
			pool.createIndexAlias(config.readName(), indexNames, currentPartition);
		}
		catch (Exception e) {
			if (mentionsAliasCollision(e)) {
				throw new IllegalStateException("Alias " + config.readName() + " cannot be created because an index with that name exists. "
						+ "monthlyPartitions() cannot be enabled on an existing single index name, use a new indexName or migrate the old index", e);
			}
			throw e;
		}
		storage.setReadyIndex(currentPartition);
	}

	/** The alias is the partition registry giving its members, oldest first. Empty when not partitioned. */
	public List<String> existingPartitions() throws Exception {
		if (!config.isPartitioned()) {
			return List.of();
		}
		return alias().map(alias -> IndexAliasUtil.getIndexNames(alias).stream().sorted().toList()).orElse(List.of());
	}

	/**
	 * Adds a month to the alias without moving the write index, for backfills. Storage must exist. The alias rewrite is a read then
	 * write with no compare and set on the server, so run backfills when no other node is creating storage.
	 */
	public void ensurePartition(YearMonth month) throws Exception {
		String partition = config.partitionName(month);
		IndexAlias alias = alias().orElseThrow(() -> new IllegalStateException("Alias " + config.readName() + " does not exist, run ensureStorage first"));
		pool.createIndex(config.clientIndexConfig(partition));
		List<String> members = IndexAliasUtil.getIndexNames(alias);
		if (!members.contains(partition)) {
			List<String> indexNames = mergeIndexes(members, partition);
			pool.createIndexAlias(config.readName(), indexNames, alias.getWriteIndex());
		}
	}

	public RecordResult record(Signal signal) {
		if (stamp != null) {
			Signal.Builder builder = signal.toBuilder();
			stamp.accept(builder);
			signal = builder.build();
		}
		Document document = enricher.toDocument(signal);
		if (failurePolicy == RecordFailurePolicy.LOG_AND_DROP) {
			if (writer.enqueue(signal, document)) {
				return new RecordResult(signal.signalId(), true);
			}
			droppedSignalLog.dropped(signal, new IllegalStateException(
					"Signal queue is full, " + SignalWriter.MAX_QUEUED + " signals are waiting to be stored to " + config.writeName()));
			return new RecordResult(signal.signalId(), false);
		}
		try {
			ensureCurrentStorage();
			pool.store(store(signal, document));
		}
		catch (Exception e) {
			throw new SignalsException("Storing signal " + signal.signalId() + " to " + config.writeName() + " failed", e);
		}
		return new RecordResult(signal.signalId(), true);
	}

	// the writer thread's store, every failure is a counted drop
	private void write(Signal signal, Document document) {
		try {
			ensureCurrentStorage();
			pool.store(store(signal, document));
		}
		catch (Exception e) {
			droppedSignalLog.dropped(signal, e);
		}
	}

	private Store store(Signal signal, Document document) {
		Store store = new Store(signal.signalId(), config.writeName());
		store.setResultDocument(document);
		return store;
	}

	/** Under PROPAGATE a failure part way throws and the results of the signals already stored are lost. The batch store API replaces this. */
	public List<RecordResult> recordAll(Collection<Signal> signals) {
		List<RecordResult> results = new ArrayList<>(signals.size());
		for (Signal signal : signals) {
			results.add(record(signal));
		}
		return results;
	}

	/** A search against the read name, so the alias when partitioned. */
	public SearchResult search(Consumer<Search> query) throws Exception {
		Search search = new Search(config.readName());
		query.accept(search);
		return pool.search(search);
	}

	/** Deletes the signals on a result page. Grouped by the physical index each hit came from, so this is right through the alias. */
	public void deleteSignals(SearchResult page) throws Exception {
		if (page.getCompleteResults().isEmpty()) {
			return;
		}
		pool.batchDelete(new BatchDelete().deleteDocumentFromQueryResult(page));
	}

	/** Rewrites the alias without the partition, then deletes it and returns the signals it held. Refuses the alias's write index. */
	public long dropPartition(String partition) throws Exception {
		if (!config.isPartitioned()) {
			throw new IllegalStateException("Index " + config.indexName() + " is not partitioned, dropPartition needs monthlyPartitions()");
		}
		if (config.partitionMonth(partition).isEmpty()) {
			throw new IllegalArgumentException(
					"Index " + partition + " is not a partition of " + config.indexName() + ", partitions are named " + config.indexName() + "-YYYY-MM");
		}
		IndexAlias alias = alias().orElseThrow(() -> new IllegalStateException("Alias " + config.readName() + " does not exist, nothing to drop"));
		List<String> members = IndexAliasUtil.getIndexNames(alias);
		if (!members.contains(partition)) {
			throw new IllegalArgumentException("Index " + partition + " is not a member of alias " + config.readName() + ", members are " + members);
		}
		if (partition.equals(alias.getWriteIndex())) {
			throw new IllegalStateException("Partition " + partition + " is the write index of alias " + config.readName()
					+ ", run ensureStorage to move writes to the current month first");
		}
		List<String> remaining = members.stream().filter(name -> !name.equals(partition)).sorted().toList();
		// a realtime search, the document count is not realtime and would miss signals stored since the last refresh
		long signals = pool.search(new Search(partition).setAmount(0).setRealtime(true)).getTotalHits();
		pool.createIndexAlias(config.readName(), remaining, alias.getWriteIndex());
		pool.deleteIndex(partition);
		return signals;
	}

	/** The actor id as this client stores it, a pseudonym under a mapping {@link ActorIdMapper}. */
	public String storedActorId(String app, String actorId) {
		return enricher.storedActorId(app, actorId);
	}

	public SignalsIndexConfig config() {
		return config;
	}

	public Clock clock() {
		return clock;
	}

	public long droppedSignals() {
		return droppedSignalLog.droppedTotal();
	}

	/** Signals queued for the writer and not yet stored. */
	public int queuedSignals() {
		return writer.queued();
	}

	/** Stops queuing and waits up to five seconds for queued signals to be stored. */
	@Override
	public void close() {
		writer.close();
	}

	// storage is created on first use and again when the month's partition changes. A failed attempt is not retried within the interval.
	// Two threads may both create, the server treats a repeated create as a no-op.
	private void ensureCurrentStorage() throws Exception {
		String target = config.isPartitioned() ? currentPartition() : config.indexName();
		if (target.equals(storage.getReadyIndex())) {
			return;
		}
		Instant now = clock.instant();
		Instant lastAttempt = storage.getLastAttempt();
		if (lastAttempt != null && Duration.between(lastAttempt, now).compareTo(SETUP_RETRY_INTERVAL) < 0) {
			return;
		}
		storage.setLastAttempt(now);
		createStorage();
	}

	private String currentPartition() {
		return config.partitionName(YearMonth.from(clock.instant().atZone(config.zone())));
	}

	private Optional<IndexAlias> alias() throws Exception {
		return pool.getIndexAliases().stream().filter(alias -> alias.getAliasName().equals(config.readName())).findFirst();
	}

	private static boolean mentionsAliasCollision(Throwable e) {
		for (Throwable cause = e; cause != null; cause = cause.getCause()) {
			if (cause.getMessage() != null && cause.getMessage().contains("collides with an existing index")) {
				return true;
			}
		}
		return false;
	}

	private static List<String> mergeIndexes(List<String> partitions, String partition) {
		return Stream.concat(partitions.stream(), Stream.of(partition)).distinct().sorted().toList();
	}
}

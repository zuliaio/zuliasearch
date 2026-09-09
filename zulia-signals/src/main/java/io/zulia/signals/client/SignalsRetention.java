package io.zulia.signals.client;

import io.zulia.client.command.factory.InstantRangeFilter;
import io.zulia.client.command.factory.RangeBehavior;
import io.zulia.client.result.SearchResult;
import io.zulia.signals.storage.SignalField;

import java.time.Instant;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Zulia has no delete by query yet, so single index mode pages through the timestamp range and batch deletes. Partitioned mode drops
 * whole months.
 */
public final class SignalsRetention {

	static final int PAGE_SIZE = 500;

	private final SignalsClient client;

	public SignalsRetention(SignalsClient client) {
		this.client = client;
	}

	public RetentionResult deleteBefore(Instant cutoff) throws Exception {
		Instant now = client.clock().instant();
		if (cutoff.isAfter(now)) {
			throw new IllegalArgumentException("Retention cutoff must not be in the future, cutoff is " + cutoff + " and now is " + now);
		}
		return client.config().isPartitioned() ? dropPartitionsBefore(cutoff) : deleteSignalsBefore(cutoff);
	}

	private RetentionResult deleteSignalsBefore(Instant cutoff) throws Exception {
		long deleted = 0;
		List<String> previousPage = List.of();
		while (true) {
			SearchResult page = client.search(search -> {
				search.setAmount(PAGE_SIZE).setRealtime(true);
				search.addQuery(new InstantRangeFilter(SignalField.TIMESTAMP.fieldName()).setMaxValue(cutoff).setEndpointBehavior(RangeBehavior.INCLUDE_MIN));
			});
			List<String> ids = page.getUniqueIds();
			if (ids.isEmpty()) {
				return new RetentionResult(deleted, List.of());
			}
			if (Set.copyOf(ids).equals(Set.copyOf(previousPage))) {
				throw new IllegalStateException("Retention made no progress on index " + client.config().indexName() + ", the same " + ids.size()
						+ " signals came back after a batch delete");
			}
			client.deleteSignals(page);
			deleted += ids.size();
			previousPage = ids;
		}
	}

	private RetentionResult dropPartitionsBefore(Instant cutoff) throws Exception {
		YearMonth cutoffMonth = YearMonth.from(cutoff.atZone(client.config().zone()));
		client.createStorage();
		long deleted = 0;
		List<String> dropped = new ArrayList<>();
		for (String partition : client.existingPartitions()) {
			YearMonth month = client.config().partitionMonth(partition)
					.orElseThrow(() -> new IllegalStateException("Alias member " + partition + " is not a partition of " + client.config().indexName()));
			if (month.isBefore(cutoffMonth)) {
				deleted += client.dropPartition(partition);
				dropped.add(partition);
			}
		}
		return new RetentionResult(deleted, List.copyOf(dropped));
	}
}

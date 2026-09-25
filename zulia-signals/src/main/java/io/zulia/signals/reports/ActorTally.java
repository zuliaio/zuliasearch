package io.zulia.signals.reports;

import java.util.Map;

/**
 * One actor's signal counts per column, an {@link Activity} from {@code tally} or a tag value from {@code tallyBy}. The actor id is the
 * stored id, so a pseudonym under a mapping {@code ActorIdMapper}.
 */
public record ActorTally<K>(String actorId, Map<K, Long> counts) {

	public ActorTally {
		if (actorId == null || actorId.isBlank()) {
			throw new IllegalArgumentException("Actor id is required for an actor tally but was " + (actorId == null ? "null" : "blank"));
		}
		if (counts == null) {
			throw new IllegalArgumentException("Counts are required for actor " + actorId + " but were null, pass an empty map for no signals");
		}
		counts = Map.copyOf(counts);
	}

	/** Zero when the actor has no signal in the column. */
	public long count(K column) {
		return counts.getOrDefault(column, 0L);
	}

	/** Over a list tag this can exceed the actor's signals, since a signal counts once per element. */
	public long total() {
		return counts.values().stream().mapToLong(Long::longValue).sum();
	}
}

package io.zulia.signals.reports;

import java.util.Map;

/** One actor's signal counts per activity. The actor id is the stored id, so a pseudonym under a mapping {@code ActorIdMapper}. */
public record ActorActivity(String actorId, Map<Activity, Long> counts) {

	public ActorActivity {
		if (actorId == null || actorId.isBlank()) {
			throw new IllegalArgumentException("Actor id is required for an actor activity but was " + (actorId == null ? "null" : "blank"));
		}
		if (counts == null) {
			throw new IllegalArgumentException("Counts are required for actor " + actorId + " but were null, pass an empty map for no activity");
		}
		counts = Map.copyOf(counts);
	}

	/** Zero when the actor has no signal for the activity. */
	public long count(Activity activity) {
		return counts.getOrDefault(activity, 0L);
	}

	public long total() {
		return counts.values().stream().mapToLong(Long::longValue).sum();
	}
}

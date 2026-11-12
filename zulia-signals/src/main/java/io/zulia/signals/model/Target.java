package io.zulia.signals.model;

import java.util.LinkedHashSet;
import java.util.List;

/** What an action touched. A bulk names every id, so reports by target count each id once while reports by action count the signal once. */
public record Target(String type, List<String> ids) {

	/** The reports' distinct count limit. Past it the honest record is one signal with a count tag, or a split bulk. */
	public static final int MAX_IDS = 10_000;

	public Target {
		String problem = problem(type, ids);
		if (problem != null) {
			throw new IllegalArgumentException(problem);
		}
		ids = List.copyOf(new LinkedHashSet<>(ids));
	}

	public Target(String type, String id) {
		this(type, id == null ? null : List.of(id));
	}

	/** Why the constructor would refuse these arguments, or null. The signal builder records it instead of throwing. */
	static String problem(String type, List<String> ids) {
		if (type == null || type.isBlank()) {
			return "Target type is required but was " + (type == null ? "null" : "blank");
		}
		if (ids == null || ids.isEmpty()) {
			return "Target id is required for target type " + type + " but was " + (ids == null ? "null" : "empty");
		}
		for (int i = 0; i < ids.size(); i++) {
			String id = ids.get(i);
			if (id == null || id.isBlank()) {
				return "Target id " + i + " of " + ids.size() + " for target type " + type + " is " + (id == null ? "null" : "blank");
			}
		}
		if (ids.size() > MAX_IDS) {
			int distinct = new LinkedHashSet<>(ids).size();
			if (distinct > MAX_IDS) {
				return "Target type " + type + " has " + distinct + " distinct ids but at most " + MAX_IDS
						+ " fit one signal, split the bulk or record the count as a tag";
			}
		}
		return null;
	}
}

package io.zulia.signals.model;

import java.util.LinkedHashSet;
import java.util.List;

/** What an action touched. A bulk names every id, so reports by target count each id once while reports by action count the signal once. */
public record Target(String type, List<String> ids) {

	/** The reports' distinct count limit. Past it the honest record is one signal with a count tag, or a split bulk. */
	public static final int MAX_IDS = 10_000;

	public Target {
		if (type == null || type.isBlank()) {
			throw new IllegalArgumentException("Target type is required but was " + (type == null ? "null" : "blank"));
		}
		if (ids == null || ids.isEmpty()) {
			throw new IllegalArgumentException("Target id is required for target type " + type + " but was " + (ids == null ? "null" : "empty"));
		}
		for (int i = 0; i < ids.size(); i++) {
			String id = ids.get(i);
			if (id == null || id.isBlank()) {
				throw new IllegalArgumentException("Target id " + i + " of " + ids.size() + " for target type " + type + " is " + (id == null ? "null" : "blank"));
			}
		}
		ids = List.copyOf(new LinkedHashSet<>(ids));
		if (ids.size() > MAX_IDS) {
			throw new IllegalArgumentException(
					"Target type " + type + " has " + ids.size() + " distinct ids but at most " + MAX_IDS + " fit one signal, split the bulk or record the count as a tag");
		}
	}

	public Target(String type, String id) {
		if (id == null || id.isBlank()) {
			throw new IllegalArgumentException("Target id is required for target type " + type + " but was " + (id == null ? "null" : "blank"));
		}
		this(type, List.of(id));
	}
}

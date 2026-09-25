package io.zulia.signals.model;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

public final class Signal {

	private final String signalId;
	private final Instant timestamp;
	private final String app;
	private final String client;
	private final String sessionId;
	private final Actor actor;
	private final String actionType;
	private final Target target;
	private final SearchDetails search;
	private final Long durationMs;
	private final Map<String, Object> tags;

	private Signal(Builder builder) {
		this.signalId = builder.signalId;
		this.timestamp = builder.timestamp;
		this.app = builder.app;
		this.client = builder.client;
		this.sessionId = builder.sessionId;
		this.actor = builder.actor;
		this.actionType = builder.actionType;
		this.target = builder.target;
		this.search = builder.search;
		this.durationMs = builder.durationMs;
		this.tags = Map.copyOf(builder.tags);
	}

	public static Builder builder() {
		return new Builder();
	}

	/** A builder holding this signal, id and timestamp included. */
	public Builder toBuilder() {
		Builder builder = new Builder();
		builder.signalId = signalId;
		builder.timestamp = timestamp;
		builder.app = app;
		builder.client = client;
		builder.sessionId = sessionId;
		builder.actor = actor;
		builder.actionType = actionType;
		builder.target = target;
		builder.search = search;
		builder.durationMs = durationMs;
		builder.tags.putAll(tags);
		return builder;
	}

	public String signalId() {
		return signalId;
	}

	public Instant timestamp() {
		return timestamp;
	}

	public String app() {
		return app;
	}

	public String client() {
		return client;
	}

	public String sessionId() {
		return sessionId;
	}

	public Actor actor() {
		return actor;
	}

	public String actionType() {
		return actionType;
	}

	public Target target() {
		return target;
	}

	public SearchDetails search() {
		return search;
	}

	public Long durationMs() {
		return durationMs;
	}

	/** Values are String, Long, Boolean, Date, or an immutable List of String. */
	public Map<String, Object> tags() {
		return tags;
	}

	@Override
	public boolean equals(Object o) {
		return o instanceof Signal other && signalId.equals(other.signalId) && timestamp.equals(other.timestamp) && app.equals(other.app)
				&& Objects.equals(client, other.client) && Objects.equals(sessionId, other.sessionId) && actor.equals(other.actor)
				&& actionType.equals(other.actionType) && Objects.equals(target, other.target) && Objects.equals(search, other.search)
				&& Objects.equals(durationMs, other.durationMs) && tags.equals(other.tags);
	}

	@Override
	public int hashCode() {
		return Objects.hash(signalId, timestamp, app, client, sessionId, actor, actionType, target, search, durationMs, tags);
	}

	@Override
	public String toString() {
		// no actor id and no query text, a signal may be logged by an app that pseudonymizes both
		return "Signal[" + signalId + " " + timestamp + " app=" + app + " client=" + client + " session=" + sessionId + " actorType=" + actor.type()
				+ " action=" + actionType + " target=" + target + " searchId=" + (search == null ? null : search.searchId()) + " durationMs=" + durationMs
				+ " tagKeys=" + tags.keySet() + "]";
	}

	public static final class Builder {

		private String signalId;
		private Instant timestamp;
		private String app;
		private String client;
		private String sessionId;
		private Actor actor;
		private String actionType;
		private Target target;
		private SearchDetails search;
		private Long durationMs;
		private final Map<String, Object> tags = new LinkedHashMap<>();

		private Builder() {
		}

		/** Overrides the generated id */
		public Builder signalId(String signalId) {
			this.signalId = signalId;
			return this;
		}

		/** Defaults to now at build time. */
		public Builder timestamp(Instant timestamp) {
			this.timestamp = timestamp;
			return this;
		}

		public Builder app(String app) {
			this.app = app;
			return this;
		}

		public Builder client(String client) {
			this.client = client == null || client.isBlank() ? null : client;
			return this;
		}

		public Builder session(String sessionId) {
			this.sessionId = sessionId == null || sessionId.isBlank() ? null : sessionId;
			return this;
		}

		/** A session id hashed from a secret such as the bearer token, see {@link SessionIds#hashed(String)}. */
		public Builder sessionHashed(String secret) {
			return session(SessionIds.hashed(secret));
		}

		public Builder actor(Actor actor) {
			this.actor = actor;
			return this;
		}

		public Builder action(String actionType) {
			this.actionType = actionType;
			return this;
		}

		public Builder target(String type, String id) {
			this.target = new Target(type, id);
			return this;
		}

		/** A bulk, one action on every id. Reports by target count each id once, reports by action count the signal once. */
		public Builder target(String type, Collection<String> ids) {
			this.target = new Target(type, ids == null ? null : new ArrayList<>(ids));
			return this;
		}

		public Builder target(Target target) {
			this.target = target;
			return this;
		}

		public Builder search(SearchDetails search) {
			this.search = search;
			return this;
		}

		public Builder search(Consumer<SearchDetails.Builder> details) {
			SearchDetails.Builder searchBuilder = search == null ? SearchDetails.builder() : search.toBuilder();
			details.accept(searchBuilder);
			this.search = searchBuilder.build();
			return this;
		}
		public Builder searchId(String searchId) {
			return searchId == null ? this : search(sd -> sd.searchId(searchId));
		}

		public Builder duration(Duration duration) {
			if (duration == null) {
				throw new IllegalArgumentException("Duration must not be null, skip the call to leave it unset");
			}
			if (duration.isNegative()) {
				throw new IllegalArgumentException("Duration must not be negative but was " + duration);
			}
			this.durationMs = duration.toMillis();
			return this;
		}

		/** Stored in the tags sub document. Index the key on the index config to filter, count, or tally on it. */
		public Builder tag(String key, String value) {
			requireKey(key);
			if (value == null || value.isBlank()) {
				throw new IllegalArgumentException("Tag " + key + " must have a value but was " + (value == null ? "null" : "blank"));
			}
			tags.put(key, value);
			return this;
		}

		/** Same as tag, skipping a null or blank value. */
		public Builder tagIfPresent(String key, String value) {
			return value == null || value.isBlank() ? this : tag(key, value);
		}

		public Builder tagIfPresent(String key, Enum<?> value) {
			return value == null ? this : tag(key, value);
		}

		public Builder tag(String key, long value) {
			tags.put(requireKey(key), value);
			return this;
		}

		public Builder tag(String key, boolean value) {
			tags.put(requireKey(key), value);
			return this;
		}

		/** Stores the constant's name. */
		public Builder tag(String key, Enum<?> value) {
			return tag(key, value == null ? null : value.name());
		}

		/** A multivalued tag. Indexed as a keyword, it facets once per element. Empty is a value, an empty list is stored. */
		public Builder tag(String key, Collection<String> values) {
			requireKey(key);
			if (values == null) {
				throw new IllegalArgumentException("Tag " + key + " must have a value but was null, pass an empty list for no values");
			}
			if (values.stream().anyMatch(value -> value == null || value.isBlank())) {
				throw new IllegalArgumentException("Tag " + key + " must not contain null or blank values but got " + values);
			}
			tags.put(key, List.copyOf(values));
			return this;
		}

		public Builder tag(String key, Instant value) {
			requireKey(key);
			if (value == null) {
				throw new IllegalArgumentException("Tag " + key + " must have a value but was null");
			}
			tags.put(key, Date.from(value));
			return this;
		}

		private static String requireKey(String key) {
			if (key == null || key.isBlank()) {
				throw new IllegalArgumentException("Tag key is required but was " + (key == null ? "null" : "blank"));
			}
			if (key.indexOf('.') >= 0 || key.indexOf('$') >= 0) {
				throw new IllegalArgumentException("Tag key " + key + " must not contain '.' or '$', it becomes a field of the tags document");
			}
			return key;
		}

		public Signal build() {
			requireText(app, "App");
			requireText(actionType, "Action");
			if (actor == null) {
				throw new IllegalArgumentException("Actor is required to build a Signal but was not set");
			}
			if (signalId == null) {
				signalId = UUID.randomUUID().toString();
			}
			if (timestamp == null) {
				timestamp = Instant.now();
			}
			return new Signal(this);
		}

		private static void requireText(String value, String name) {
			if (value == null || value.isBlank()) {
				throw new IllegalArgumentException(name + " is required to build a Signal but was " + (value == null ? "not set" : "blank"));
			}
		}
	}
}

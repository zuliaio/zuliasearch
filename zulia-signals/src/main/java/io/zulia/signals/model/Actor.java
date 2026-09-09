package io.zulia.signals.model;

public record Actor(String id, ActorType type, String delegatedBy) {

	public static final String SYSTEM_ID = "system";

	public Actor {
		if (type == null) {
			throw new IllegalArgumentException("Actor type is required");
		}
		if (id == null || id.isBlank()) {
			throw new IllegalArgumentException("Actor id is required for actor type " + type + " but was " + (id == null ? "null" : "blank"));
		}
		if (delegatedBy != null && type != ActorType.AGENT) {
			throw new IllegalArgumentException("Only AGENT actors carry delegatedBy, but actor type is " + type + " with delegatedBy " + delegatedBy);
		}
	}

	public static Actor user(String id) {
		return new Actor(id, ActorType.USER, null);
	}

	public static Actor anonymous(String sessionId) {
		return new Actor(sessionId, ActorType.ANONYMOUS, null);
	}

	public static Actor agent(String id, String delegatedBy) {
		return new Actor(id, ActorType.AGENT, delegatedBy);
	}

	public static Actor agent(String id) {
		return new Actor(id, ActorType.AGENT, null);
	}

	public static Actor service(String id) {
		return new Actor(id, ActorType.SERVICE, null);
	}

	public static Actor system() {
		return new Actor(SYSTEM_ID, ActorType.SYSTEM, null);
	}
}

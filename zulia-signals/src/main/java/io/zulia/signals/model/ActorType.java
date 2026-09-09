package io.zulia.signals.model;

public enum ActorType {
	USER,
	ANONYMOUS,
	/** An AI agent acting for a human, carries delegatedBy. */
	AGENT,
	/**
	 * Another application calling under its own machine identity, an API key owner or a batch job of another system. No person is
	 * behind the call, so there is no delegatedBy. Unlike SYSTEM it counts in reports, since a service using the platform is usage.
	 */
	SERVICE,
	/** The platform itself: jobs, reindexes, health checks. Excluded from usage reports. */
	SYSTEM
}

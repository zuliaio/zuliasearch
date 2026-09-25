package io.zulia.signals.client;

/**
 * Accepted is true when the signal was stored, or under {@link RecordFailurePolicy#LOG_AND_DROP} when it was queued for the writer.
 * A queued signal that later fails counts in {@code SignalsClient.droppedSignals()}. The id is null when a builder failed to build.
 */
public record RecordResult(String signalId, boolean accepted) {
}

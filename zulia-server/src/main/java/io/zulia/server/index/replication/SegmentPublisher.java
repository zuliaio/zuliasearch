package io.zulia.server.index.replication;

public interface SegmentPublisher {

	void publish(PublishRequest request) throws Exception;

	default void close() {
	}
}

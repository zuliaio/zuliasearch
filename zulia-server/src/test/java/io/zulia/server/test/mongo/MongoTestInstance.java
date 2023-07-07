package io.zulia.server.test.mongo;

import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.embed.process.runtime.Network;
import de.flapdoodle.reverse.TransitionWalker;
import de.flapdoodle.reverse.transitions.Start;

public class MongoTestInstance {

	private static final String LOCAL_INSTANCE_URL_HOST = "mongodb://127.0.0.1";

	private Integer port;
	private String testInstanceUrl;

	private TransitionWalker.ReachedState<RunningMongodProcess> running;

	public MongoTestInstance() {
	}

	public String getInstanceUrl() {
		return testInstanceUrl;
	}

	public void shutdown() {
		if (running != null) {
			running.close();
		}
	}

	public void start() {

		try {
			port = Network.freeServerPort(de.flapdoodle.net.Net.getLocalHost());
			running = Mongod.builder().net(Start.to(Net.class).initializedWith(Net.defaults().withPort(port))).build().start(Version.Main.V6_0);
			testInstanceUrl = buildTestInstanceUrl();

		}
		catch (Exception ex) {
			throw new IllegalStateException("Unable to start the test MongoDB instance", ex);
		}

	}

	private String buildTestInstanceUrl() {
		return LOCAL_INSTANCE_URL_HOST + ":" + port;
	}

}


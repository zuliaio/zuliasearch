package io.zulia.server.test.mongo;

import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.embed.process.io.ImmutableProcessOutput;
import de.flapdoodle.embed.process.io.ProcessOutput;
import de.flapdoodle.embed.process.io.Processors;
import de.flapdoodle.embed.process.io.Slf4jLevel;
import de.flapdoodle.reverse.TransitionWalker;
import de.flapdoodle.reverse.transitions.Start;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoTestInstance {

	private final static Logger LOG = LoggerFactory.getLogger(MongoTestInstance.class);

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

			ImmutableProcessOutput processOutput = ImmutableProcessOutput.builder().output(Processors.logTo(LOG, Slf4jLevel.DEBUG))
					.error(Processors.logTo(LOG, Slf4jLevel.ERROR)).commands(Processors.named("[console>]", Processors.logTo(LOG, Slf4jLevel.DEBUG))).build();

			port = de.flapdoodle.net.Net.freeServerPort(de.flapdoodle.net.Net.getLocalHost());
			running = Mongod.builder().processOutput(Start.to(ProcessOutput.class).initializedWith(processOutput))
					.net(Start.to(Net.class).initializedWith(Net.defaults().withPort(port))).build().start(Version.Main.V6_0);
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


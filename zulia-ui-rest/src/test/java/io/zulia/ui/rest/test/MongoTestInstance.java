package io.zulia.ui.rest.test;

import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.embed.process.io.ImmutableProcessOutput;
import de.flapdoodle.embed.process.io.ProcessOutput;
import de.flapdoodle.embed.process.io.Processors;
import de.flapdoodle.embed.process.io.Slf4jLevel;
import de.flapdoodle.commons.reverse.TransitionWalker;
import de.flapdoodle.commons.reverse.transitions.Start;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoTestInstance {

	private static final Logger LOG = LoggerFactory.getLogger(MongoTestInstance.class);
	private static final String LOCAL_INSTANCE_URL_HOST = "mongodb://127.0.0.1";

	private Integer port;
	private String testInstanceUrl;
	private TransitionWalker.ReachedState<RunningMongodProcess> running;

	public String getInstanceUrl() {
		return testInstanceUrl;
	}

	public void start() {
		try {
			ImmutableProcessOutput processOutput = ImmutableProcessOutput.builder()
					.output(Processors.logTo(LOG, Slf4jLevel.DEBUG))
					.error(Processors.logTo(LOG, Slf4jLevel.ERROR))
					.commands(Processors.named("[console>]", Processors.logTo(LOG, Slf4jLevel.DEBUG)))
					.build();

			port = de.flapdoodle.commons.net.Net.freeServerPort(de.flapdoodle.commons.net.Net.getLocalHost());
			// MongoDB 8.x binaries (8.0.23 through 8.3.2) refuse to start on Linux kernel 6.19 and newer because of a
			// TCMalloc rseq incompatibility (SERVER-121912). The fixed binaries only allow kernel 7.0.14 and above
			// (SERVER-125742). Stay on 7.0 until the dev and CI kernels are past that line.
			running = Mongod.builder()
					.processOutput(Start.to(ProcessOutput.class).initializedWith(processOutput))
					.net(Start.to(Net.class).initializedWith(Net.defaults().withPort(port)))
					.build()
					.start(Version.Main.V7_0);
			testInstanceUrl = LOCAL_INSTANCE_URL_HOST + ":" + port;
		}
		catch (Exception ex) {
			throw new IllegalStateException("Unable to start the test MongoDB instance", ex);
		}
	}

	public void shutdown() {
		if (running != null) {
			running.close();
		}
	}
}

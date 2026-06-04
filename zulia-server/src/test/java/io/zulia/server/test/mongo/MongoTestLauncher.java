package io.zulia.server.test.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.zulia.server.util.MongoProvider;
import org.apache.commons.lang3.StringUtils;
import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;

/**
 * Owns the embedded MongoDB and the shared test MongoClient for the whole JVM test run.
 *
 * Runs inside the JUnit launcher lifecycle rather than a JVM shutdown hook, so teardown is
 * deterministic and ordered: the client (and its monitor threads) is closed while mongod is
 * still alive, then mongod is stopped - all before the JVM shutdown phase. This removes the
 * InterruptedAtShutdown (11600) heartbeat noise that the old shutdown hook produced by racing
 * Flapdoodle's own shutdown hook.
 */
public class MongoTestLauncher implements LauncherSessionListener {

	public static final String MONGO_SERVER_PROPERTY = "zulia.test.mongoServer";

	private static final String MONGO_TEST_CONNECTION = "mongoTestConnection";
	private static final String MONGO_TEST_CONNECTION_DEFAULT = "mongodb://127.0.0.1:27017";

	private MongoTestInstance embeddedMongo;

	@Override
	public void launcherSessionOpened(LauncherSession session) {
		String externalConnection = System.getProperty(MONGO_TEST_CONNECTION);

		String mongoServer;
		if (StringUtils.isEmpty(externalConnection)) {
			embeddedMongo = new MongoTestInstance();
			embeddedMongo.start();
			mongoServer = embeddedMongo.getInstanceUrl();
		}
		else {
			mongoServer = externalConnection;
		}

		System.setProperty(MONGO_SERVER_PROPERTY, mongoServer);
		MongoProvider.setMongoClient(MongoClients.create(mongoServer));
	}

	@Override
	public void launcherSessionClosed(LauncherSession session) {
		MongoClient client = MongoProvider.getMongoClient();
		if (client != null) {
			// Close while mongod is still up so no heartbeat hits a shutting-down server.
			client.close();
		}
		if (embeddedMongo != null) {
			embeddedMongo.shutdown();
		}
	}
}

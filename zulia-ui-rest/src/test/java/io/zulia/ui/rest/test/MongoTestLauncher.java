package io.zulia.ui.rest.test;

import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;

public class MongoTestLauncher implements LauncherSessionListener {

	private MongoTestInstance mongo;

	@Override
	public void launcherSessionOpened(LauncherSession session) {
		mongo = new MongoTestInstance();
		mongo.start();
		System.setProperty("mongodb.uri", mongo.getInstanceUrl() + "/test");
	}

	@Override
	public void launcherSessionClosed(LauncherSession session) {
		if (mongo != null) {
			mongo.shutdown();
		}
	}
}

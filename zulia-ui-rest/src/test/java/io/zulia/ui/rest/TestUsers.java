package io.zulia.ui.rest;

import io.micronaut.runtime.server.EmbeddedServer;
import io.zulia.ui.rest.beans.UserEntity;
import io.zulia.ui.rest.persistence.MongoUserPersistence;

import static io.zulia.ui.rest.TestConstants.PASSWORD;
import static io.zulia.ui.rest.TestConstants.USERNAME;

public final class TestUsers {

	private TestUsers() {
	}

	public static void ensureTestUser(EmbeddedServer embeddedServer) {
		MongoUserPersistence persistence = embeddedServer.getApplicationContext().getBean(MongoUserPersistence.class);
		if (persistence.getUser(USERNAME) == null) {
			UserEntity user = new UserEntity();
			user.setUsername(USERNAME);
			user.setPassword(PASSWORD);
			persistence.createUser(user);
		}
	}
}

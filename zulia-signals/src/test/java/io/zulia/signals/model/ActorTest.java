package io.zulia.signals.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ActorTest {

	@Test
	void factoriesSetTypes() {
		Assertions.assertEquals(ActorType.USER, Actor.user("u1").type());
		Assertions.assertEquals(ActorType.ANONYMOUS, Actor.anonymous("s1").type());
		Assertions.assertEquals("u1", Actor.agent("zulia-agent", "u1").delegatedBy());
		Assertions.assertEquals(ActorType.SERVICE, Actor.service("reporting-service").type());
		Assertions.assertEquals(Actor.SYSTEM_ID, Actor.system().id());
	}

	@Test
	void delegationOnlyForAgents() {
		IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class, () -> new Actor("u1", ActorType.USER, "u2"));
		Assertions.assertTrue(error.getMessage().contains("USER") && error.getMessage().contains("u2"), error.getMessage());
	}

	@Test
	void blankIdNamesTheType() {
		IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class, () -> Actor.service(" "));
		Assertions.assertTrue(error.getMessage().contains("SERVICE") && error.getMessage().contains("blank"), error.getMessage());
	}
}

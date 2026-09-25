package io.zulia.signals.reports;

import io.zulia.signals.model.Actions;
import io.zulia.signals.model.Targets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class ActivityTest {

	@Test
	void anyTargetHasNoTargetType() {
		Assertions.assertNull(Activity.of(Actions.LOGIN).targetType());
		Assertions.assertEquals(Targets.PROJECT, Activity.of(Actions.CREATE, Targets.PROJECT).targetType());
	}

	@Test
	void rejectsBlankParts() {
		Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> Activity.of(" ")).getMessage().contains("blank"));
		Assertions.assertThrows(IllegalArgumentException.class, () -> Activity.of(null, null));
		Assertions.assertTrue(
				Assertions.assertThrows(IllegalArgumentException.class, () -> Activity.of(Actions.CREATE, null)).getMessage().contains("Activity.of(actionType)"));
		Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> new Activity(Actions.CREATE, "")).getMessage().contains("blank"));
	}

	@Test
	void actorTallyDefaultsMissingCountsToZero() {
		Activity created = Activity.of(Actions.CREATE, Targets.PROJECT);
		ActorTally<Activity> row = new ActorTally<>("u1", Map.of(created, 3L));
		Assertions.assertEquals(3, row.count(created));
		Assertions.assertEquals(0, row.count(Activity.of(Actions.VISIT, Targets.PROJECT)));
		Assertions.assertEquals(3, row.total());
		Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> new ActorTally<>("u1", null)).getMessage().contains("u1"));
		Assertions.assertThrows(IllegalArgumentException.class, () -> new ActorTally<>(" ", Map.of()));
	}
}

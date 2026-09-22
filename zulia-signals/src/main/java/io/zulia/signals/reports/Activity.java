package io.zulia.signals.reports;

/** An action, optionally on one target type, such as create on project. */
public record Activity(String actionType, String targetType) {

	public Activity {
		if (actionType == null || actionType.isBlank()) {
			throw new IllegalArgumentException("Action type is required for an activity but was " + (actionType == null ? "null" : "blank"));
		}
		if (targetType != null && targetType.isBlank()) {
			throw new IllegalArgumentException("Target type of activity " + actionType + " was blank, use Activity.of(actionType) for any target");
		}
	}

	/** The action on any target or none, such as login. */
	public static Activity of(String actionType) {
		return new Activity(actionType, null);
	}

	public static Activity of(String actionType, String targetType) {
		if (targetType == null) {
			throw new IllegalArgumentException("Target type of activity " + actionType + " was null, use Activity.of(actionType) for any target");
		}
		return new Activity(actionType, targetType);
	}
}

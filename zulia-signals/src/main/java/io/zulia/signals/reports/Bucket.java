package io.zulia.signals.reports;

import io.zulia.signals.storage.SignalField;

public enum Bucket {
	DAY(SignalField.DAY),
	WEEK(SignalField.WEEK),
	MONTH(SignalField.MONTH);

	private final SignalField field;

	Bucket(SignalField field) {
		this.field = field;
	}

	public SignalField field() {
		return field;
	}
}

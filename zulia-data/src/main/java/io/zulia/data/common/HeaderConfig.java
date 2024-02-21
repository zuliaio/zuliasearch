package io.zulia.data.common;

public class HeaderConfig {

	private boolean allowDuplicates;
	private boolean allowBlanks;

	public HeaderConfig() {

	}

	public HeaderConfig allowDuplicates(boolean allowDuplicates) {
		this.allowDuplicates = allowDuplicates;
		return this;
	}

	public HeaderConfig allowBlanks(boolean allowBlanks) {
		this.allowBlanks = allowBlanks;
		return this;
	}

	public boolean isAllowDuplicates() {
		return allowDuplicates;
	}

	public boolean isAllowBlanks() {
		return allowBlanks;
	}
}

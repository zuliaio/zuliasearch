package io.zulia.data.common;

public class HeaderConfig {

	private boolean allowDuplicates = true;
	private boolean allowBlanks = true;
	private boolean ignoreTrailingBlanks = false;

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

	/**
	 * Drops blank header cells after the last named header, which is what a spreadsheet leaves behind when formatting was
	 * applied past the data. Any value under such a cell can then only be read by index.
	 */
	public HeaderConfig ignoreTrailingBlanks(boolean ignoreTrailingBlanks) {
		this.ignoreTrailingBlanks = ignoreTrailingBlanks;
		return this;
	}

	public boolean isAllowDuplicates() {
		return allowDuplicates;
	}

	public boolean isAllowBlanks() {
		return allowBlanks;
	}

	public boolean isIgnoreTrailingBlanks() {
		return ignoreTrailingBlanks;
	}
}

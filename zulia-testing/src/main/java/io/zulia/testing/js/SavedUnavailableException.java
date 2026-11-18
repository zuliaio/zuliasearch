package io.zulia.testing.js;

/**
 * Thrown when an expression reads a saved value that no earlier run recorded. The runner reports the expression as SKIPPED.
 */
public class SavedUnavailableException extends RuntimeException {

	public SavedUnavailableException(String message) {
		super(message);
	}
}

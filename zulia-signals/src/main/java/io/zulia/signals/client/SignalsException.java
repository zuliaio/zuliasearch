package io.zulia.signals.client;

/** Recording and reporting never force a throws clause on the app, the client library's checked exception is wrapped here. */
public final class SignalsException extends RuntimeException {

	public SignalsException(String message, Throwable cause) {
		super(message, cause);
	}
}

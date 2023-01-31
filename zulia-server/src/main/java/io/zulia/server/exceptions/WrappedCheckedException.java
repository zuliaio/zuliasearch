package io.zulia.server.exceptions;

public class WrappedCheckedException extends RuntimeException {

    public WrappedCheckedException(Exception cause) {
        super(cause);
    }

    @Override
    public synchronized Exception getCause() {
        return (Exception) super.getCause();
    }
}

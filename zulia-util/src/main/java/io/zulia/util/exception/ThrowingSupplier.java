package io.zulia.util.exception;

import com.google.common.base.Supplier;

public interface ThrowingSupplier<T> extends Supplier<T> {

	T throwingGet() throws Throwable;

	@Override
	default T get() {
		try {
			return throwingGet();
		}
		catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

}

package io.zulia.util.sketch;

import java.util.Arrays;

/** Growing byte buffer that a sketch writes its byte form into, as varints and little endian doubles. */
final class SketchByteWriter {

	private byte[] bytes;
	private int n;

	SketchByteWriter(int capacity) {
		bytes = new byte[Math.max(16, capacity)];
	}

	void varLong(long v) {
		ensure(10);
		while ((v & ~0x7FL) != 0) {
			bytes[n++] = (byte) ((v & 0x7F) | 0x80);
			v >>>= 7;
		}
		bytes[n++] = (byte) v;
	}

	void fixedDouble(double v) {
		ensure(8);
		long bits = Double.doubleToRawLongBits(v);
		for (int i = 0; i < 8; i++) {
			bytes[n++] = (byte) (bits >>> (8 * i));
		}
	}

	private void ensure(int room) {
		if (n + room > bytes.length) {
			bytes = Arrays.copyOf(bytes, Math.max(bytes.length * 2, n + room));
		}
	}

	byte[] toArray() {
		return Arrays.copyOf(bytes, n);
	}
}

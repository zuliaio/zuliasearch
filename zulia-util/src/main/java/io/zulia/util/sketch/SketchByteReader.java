package io.zulia.util.sketch;

/** Reads the varints and little endian doubles that {@link SketchByteWriter} writes, front to back. */
final class SketchByteReader {

	private final byte[] bytes;
	private int n;

	SketchByteReader(byte[] bytes) {
		this.bytes = bytes;
	}

	long varLong() {
		long v = 0;
		int shift = 0;
		while (true) {
			byte b = bytes[n++];
			v |= (long) (b & 0x7F) << shift;
			if (b >= 0) {
				return v;
			}
			shift += 7;
		}
	}

	double fixedDouble() {
		long bits = 0;
		for (int i = 0; i < 8; i++) {
			bits |= (bytes[n++] & 0xFFL) << (8 * i);
		}
		return Double.longBitsToDouble(bits);
	}
}

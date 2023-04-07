package io.zulia.server.util;

import org.apache.lucene.util.BytesRef;

public class BytesRefUtil {

	public static byte[] getByteArray(BytesRef bytesRef) {
		if (bytesRef.offset != 0 || bytesRef.bytes.length != bytesRef.length) {
			return BytesRef.deepCopyOf(bytesRef).bytes;
		}
		return bytesRef.bytes;
	}
}

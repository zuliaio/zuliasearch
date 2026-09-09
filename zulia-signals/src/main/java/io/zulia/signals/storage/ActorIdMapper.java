package io.zulia.signals.storage;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;

/** Maps actor ids before storage. Identity by default, {@link #hmacSha256(byte[])} to pseudonymize. */
@FunctionalInterface
public interface ActorIdMapper {

	String map(String app, String actorId);

	static ActorIdMapper identity() {
		return (app, actorId) -> actorId;
	}

	static ActorIdMapper hmacSha256(byte[] key) {
		if (key == null || key.length < 16) {
			throw new IllegalArgumentException("HMAC key must be at least 16 bytes, got " + (key == null ? "null" : key.length + " bytes"));
		}
		SecretKeySpec keySpec = new SecretKeySpec(key, "HmacSHA256");
		return (String app, String actorId) -> {
			try {
				Mac mac = Mac.getInstance("HmacSHA256");
				mac.init(keySpec);
				mac.update(app.getBytes(StandardCharsets.UTF_8));
				mac.update((byte) 0);
				return HexFormat.of().formatHex(mac.doFinal(actorId.getBytes(StandardCharsets.UTF_8)));
			}
			catch (GeneralSecurityException e) {
				throw new IllegalStateException("HmacSHA256 is unavailable in this JVM", e);
			}
		};
	}
}

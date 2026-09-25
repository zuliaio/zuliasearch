package io.zulia.signals.model;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Session ids derived from secrets, so a bearer token groups a session without being stored. The hash is unkeyed, so the secret must be high
 * entropy like a signed token. A guessable value needs a keyed hash, see {@code ActorIdMapper.hmacSha256}.
 */
public final class SessionIds {

	private static final int HASH_BYTES = 16;

	private SessionIds() {
	}

	/** The first 16 bytes of SHA-256 over the secret as 32 hex characters. Null or blank gives null, which leaves the session unset. */
	public static String hashed(String secret) {
		if (secret == null || secret.isBlank()) {
			return null;
		}
		try {
			byte[] digest = MessageDigest.getInstance("SHA-256").digest(secret.getBytes(StandardCharsets.UTF_8));
			return HexFormat.of().formatHex(digest, 0, HASH_BYTES);
		}
		catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is unavailable in this JVM", e);
		}
	}
}

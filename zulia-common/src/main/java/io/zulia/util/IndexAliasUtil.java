package io.zulia.util;

import io.zulia.message.ZuliaIndex.IndexAlias;

import java.util.List;

public final class IndexAliasUtil {

	private IndexAliasUtil() {
	}

	public static List<String> getIndexNames(IndexAlias alias) {
		if (!alias.getIndexNamesList().isEmpty()) {
			return alias.getIndexNamesList();
		}
		if (!alias.getIndexName().isEmpty()) {
			return List.of(alias.getIndexName());
		}
		return List.of();
	}

	public static String requireSingleIndex(IndexAlias alias) {
		List<String> names = getIndexNames(alias);
		if (names.isEmpty()) {
			throw new IllegalArgumentException("Alias <" + alias.getAliasName() + "> has no index names");
		}
		if (names.size() > 1) {
			throw new IllegalArgumentException(
					"Alias <" + alias.getAliasName() + "> maps to multiple indexes " + names + " and cannot be used for single-index operations");
		}
		return names.getFirst();
	}

	public static String requireWriteIndex(IndexAlias alias) {
		String writeIndex = alias.getWriteIndex();
		if (!writeIndex.isEmpty()) {
			return writeIndex;
		}
		List<String> names = getIndexNames(alias);
		if (names.isEmpty()) {
			throw new IllegalArgumentException("Alias <" + alias.getAliasName() + "> has no index names");
		}
		if (names.size() > 1) {
			throw new IllegalArgumentException("Alias <" + alias.getAliasName() + "> maps to multiple indexes " + names
					+ " and has no designated write index; cannot be used for single-index write operations");
		}
		return names.getFirst();
	}
}

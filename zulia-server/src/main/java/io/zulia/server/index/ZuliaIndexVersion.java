package io.zulia.server.index;

/**
 * Registry for the index-creation version stamped on {@code IndexSettings.createdIndexVersion}.
 * <p>
 * The version is stamped once when an index is created and never changes afterward. It lets new indexes adopt evolving
 * built-in defaults that cannot be expressed per field and must not change for an index that already exists (for example,
 * the doc-values skip index on the built-in id sort field). Indexes created before versioning existed report 0 (legacy)
 * and keep their original behavior.
 * <p>
 * Gate a behavior on {@code createdIndexVersion >=} the fixed version in which it was introduced, never on
 * {@link #CURRENT} (which moves). Otherwise, bumping CURRENT would silently change the behavior for older versioned indexes.
 */
public interface ZuliaIndexVersion {


	/** Stamped on every newly created index. Bump when a new creation-time default is introduced. */
	int CURRENT = 2;

	/** Indexes at or above this version build a doc-values skip index on the built-in id sort field. */
	int ID_SORT_DOC_VALUE_SKIP = 1;

	/** Indexes at or above this version default dense-vector fields (with no explicit encoding) to INT8 scalar quantization. */
	int VECTOR_DEFAULT_INT8 = 2;
}

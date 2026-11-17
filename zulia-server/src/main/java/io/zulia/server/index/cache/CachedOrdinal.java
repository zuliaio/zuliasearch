package io.zulia.server.index.cache;

/** A cached taxonomy ordinal and the reader generation its category was added under. */
record CachedOrdinal(int ordinal, long generation) {
}

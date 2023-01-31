package io.zulia.client.command.factory;

/**
 * Describes endpoint behavior for search building
 */
public enum RangeBehavior {
    EXCLUSIVE,      // Exclude both end points
    INCLUSIVE,      // Include both end points
    INCLUDE_MIN,    // Include min endpoint only
    INCLUDE_MAX     // Include max endpoint only
}

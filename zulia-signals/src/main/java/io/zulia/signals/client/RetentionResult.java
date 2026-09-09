package io.zulia.signals.client;

import java.util.List;

/** signalsDeleted counts signals removed in either mode. partitionsDropped lists the months dropped, empty for a single index. */
public record RetentionResult(long signalsDeleted, List<String> partitionsDropped) {
}

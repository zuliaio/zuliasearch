package io.zulia.server.index;

import org.apache.lucene.util.BytesRef;

public record ReIndexContainer(BytesRef idInfo, BytesRef meta, BytesRef fullDoc) {
}

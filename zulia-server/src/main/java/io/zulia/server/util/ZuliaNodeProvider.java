package io.zulia.server.util;

import io.zulia.server.node.ZuliaNode;

public class ZuliaNodeProvider {

	private static ZuliaNode zuliaNode;

	public static void setZuliaNode(ZuliaNode zuliaNode) {
		ZuliaNodeProvider.zuliaNode = zuliaNode;
	}

	public static ZuliaNode getZuliaNode() {
		return zuliaNode;
	}
}

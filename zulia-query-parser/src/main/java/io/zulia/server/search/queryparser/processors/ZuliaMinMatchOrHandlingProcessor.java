package io.zulia.server.search.queryparser.processors;

import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OrQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.standard.nodes.MinShouldMatchNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Inside every MinShouldMatchNode group, rewrite "default" BooleanQueryNode that
 * isn't AndQueryNode or OrQueryNode into OrQueryNode.
 * This forces local default-OR behavior regardless of the global default operator
 * while preserving explicit AND subtrees and explicit modifiers (+, -, NOT, ...).
 * Needs to be immediately before BooleanQuery2ModifierNodeProcessor in the pipeline
 */
public class ZuliaMinMatchOrHandlingProcessor extends QueryNodeProcessorImpl {

	@Override
	protected QueryNode postProcessNode(QueryNode node) {
		if (!(node instanceof MinShouldMatchNode mm)) {
			return node;
		}

		GroupQueryNode group = mm.groupQueryNode;
		if (group == null || group.getChild() == null) {
			return node;
		}

		QueryNode rewritten = rewrite(group.getChild(), false);
		group.setChild(rewritten);
		return node;
	}

	private QueryNode rewrite(QueryNode node, boolean underExplicitAnd) {
		if (node == null)
			return null;

		// Keep explicit modifiers (+ , - , NOT , ...)
		if (node instanceof ModifierQueryNode) {
			return node;
		}

		// Preserve explicit AND subtrees but still recurse into them
		if (node instanceof AndQueryNode) {
			List<QueryNode> kids = node.getChildren();
			if (kids != null && !kids.isEmpty()) {
				List<QueryNode> newKids = new ArrayList<>(kids.size());
				for (QueryNode c : kids) {
					newKids.add(rewrite(c, true));
				}
				node.set(newKids);
			}
			return node;
		}

		// Recurse first
		List<QueryNode> kids = node.getChildren();
		if (kids != null && !kids.isEmpty()) {
			List<QueryNode> newKids = new ArrayList<>(kids.size());
			for (QueryNode c : kids) {
				newKids.add(rewrite(c, underExplicitAnd));
			}
			node.set(newKids);
		}

		// If inside an explicit AND subtree, do not coerce default boolean into OR
		if (underExplicitAnd) {
			return node;
		}

		// Coerce "default" boolean (implicit whitespace) into explicit OR
		// Default boolean shows up as BooleanQueryNode, but NOT AndQueryNode/OrQueryNode.
		if (node instanceof BooleanQueryNode && !(node instanceof OrQueryNode)) {
			List<QueryNode> children = node.getChildren();
			if (children != null && children.size() > 1) {
				return new OrQueryNode(children);
			}
		}

		return node;
	}

	@Override
	protected QueryNode preProcessNode(QueryNode node) {
		return node;
	}

	@Override
	protected List<QueryNode> setChildrenOrder(List<QueryNode> children) {
		return children;
	}
}
package io.zulia.server.search.queryparser.processors;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.FieldConfig;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchNoDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.RangeQueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointRangeQueryNode;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.List;

//modified from PointQueryNodeProcessor to return MatchNoDocsQueryNode instead of throwing a parse exception.
// this is important if a numeric field and normal field are both queried at the same time, i.e. defaultFields = title, pubYear with query "cancer"
public class ZuliaPointQueryNodeProcessor extends QueryNodeProcessorImpl {

	/** Constructs a {@link ZuliaPointQueryNodeProcessor} object. */
	public ZuliaPointQueryNodeProcessor() {
		// empty constructor
	}

	@Override
	protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

		if (node instanceof FieldQueryNode && !(node.getParent() instanceof RangeQueryNode)) {

			QueryConfigHandler config = getQueryConfigHandler();

			if (config != null) {
				FieldQueryNode fieldNode = (FieldQueryNode) node;
				FieldConfig fieldConfig = config.getFieldConfig(fieldNode.getFieldAsString());

				if (fieldConfig != null) {
					PointsConfig numericConfig = fieldConfig.get(ConfigurationKeys.POINTS_CONFIG);

					if (numericConfig != null) {

						NumberFormat numberFormat = numericConfig.getNumberFormat();
						String text = fieldNode.getTextAsString();
						Number number;

						if (text.length() > 0) {

							try {
								number = numberFormat.parse(text);

							}
							catch (ParseException e) {
								//zulia - dont throw exception, return match no docs
								return new MatchNoDocsQueryNode();
							}

							if (Integer.class.equals(numericConfig.getType())) {
								number = number.intValue();
							}
							else if (Long.class.equals(numericConfig.getType())) {
								number = number.longValue();
							}
							else if (Double.class.equals(numericConfig.getType())) {
								number = number.doubleValue();
							}
							else if (Float.class.equals(numericConfig.getType())) {
								number = number.floatValue();
							}

						}
						else {
							//zulia - dont throw exception, return match no docs
							return new MatchNoDocsQueryNode();
						}

						PointQueryNode lowerNode = new PointQueryNode(fieldNode.getField(), number, numberFormat);
						PointQueryNode upperNode = new PointQueryNode(fieldNode.getField(), number, numberFormat);

						return new PointRangeQueryNode(lowerNode, upperNode, true, true, numericConfig);
					}
				}
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

package io.zulia.server.search.queryparser.zulia.test;

import io.zulia.server.search.queryparser.zulia.ZuliaStandardQueryParser;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.Query;

import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class Test {

	public static void main(String[] args) throws QueryNodeException {
		ZuliaStandardQueryParser standardQueryParser = new ZuliaStandardQueryParser();

		standardQueryParser.setMultiFields(new String[] { "default1", "default2" });

		Map<String, PointsConfig> pointConfigMap = new HashMap<>();
		pointConfigMap.put("pubYear", new PointsConfig(NumberFormat.getIntegerInstance(Locale.ROOT), Integer.class));
		standardQueryParser.setPointsConfigMap(pointConfigMap);

		try {
			String query1 = "title,abstract:(\"lung cancer\" OR diabetes OR stuff)~2 AND b AND pubYear>=2000";
			String query2 = "\"ice cream\" AND pubYear:2000 ";

			QueryNode node = standardQueryParser.getSyntaxParser().parse(query2, null);

			Query query = standardQueryParser.parse(query1, null);

			System.out.println(query);
		}
		catch (QueryNodeException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}

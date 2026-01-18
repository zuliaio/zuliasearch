package io.zulia.queryparser.test;

import io.zulia.DefaultAnalyzers;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.search.queryparser.ZuliaFlexibleQueryParser;
import org.apache.lucene.search.Query;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class QueryParserTest {

	@Test
	public void minimumShouldMatchTest() throws Exception {

		ZuliaIndex.IndexSettings.Builder builder = ZuliaIndex.IndexSettings.newBuilder();
		builder.addFieldConfig(ZuliaIndex.FieldConfig.newBuilder()
				.addIndexAs(ZuliaIndex.IndexAs.newBuilder().setAnalyzerName(DefaultAnalyzers.STANDARD).setIndexFieldName("title").build())
				.setFieldType(ZuliaIndex.FieldConfig.FieldType.STRING).setStoredFieldName("title").build());
		builder.addFieldConfig(ZuliaIndex.FieldConfig.newBuilder()
				.addIndexAs(ZuliaIndex.IndexAs.newBuilder().setAnalyzerName(DefaultAnalyzers.STANDARD).setIndexFieldName("abstract").build())
				.setFieldType(ZuliaIndex.FieldConfig.FieldType.STRING).setStoredFieldName("abstract").build());

		ServerIndexConfig serverIndexConfig = new ServerIndexConfig(builder.build());
		ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer = new ZuliaPerFieldAnalyzer(serverIndexConfig);
		ZuliaFlexibleQueryParser zuliaFlexibleQueryParser = new ZuliaFlexibleQueryParser(zuliaPerFieldAnalyzer, serverIndexConfig);


		Query parse;

		String mmQuery = "abstract:diabetes title:(cancer AND lung -fly rat bear insect +fruit)@2";

		zuliaFlexibleQueryParser.setDefaultOperator(ZuliaQuery.Query.Operator.OR);
		parse = zuliaFlexibleQueryParser.parse(mmQuery);
		Assertions.assertEquals("abstract:diabetes ((+title:cancer +title:lung -title:fly title:rat title:bear title:insect +title:fruit)~2)",
				parse.toString());

		zuliaFlexibleQueryParser.setDefaultOperator(ZuliaQuery.Query.Operator.AND);
		parse = zuliaFlexibleQueryParser.parse(mmQuery);
		Assertions.assertEquals("+abstract:diabetes +((+title:cancer +title:lung -title:fly title:rat title:bear title:insect +title:fruit)~2)",
				parse.toString());



		//test other syntax
		String mmQuery3 = "abstract:diabetes title:(cancer AND lung -fly rat bear insect +fruit)~2";
		zuliaFlexibleQueryParser.setDefaultOperator(ZuliaQuery.Query.Operator.OR);
		parse = zuliaFlexibleQueryParser.parse(mmQuery3);
		Assertions.assertEquals("abstract:diabetes ((+title:cancer +title:lung -title:fly title:rat title:bear title:insect +title:fruit)~2)",
				parse.toString());

		zuliaFlexibleQueryParser.setDefaultOperator(ZuliaQuery.Query.Operator.AND);
		parse = zuliaFlexibleQueryParser.parse(mmQuery3);
		Assertions.assertEquals("+abstract:diabetes +((+title:cancer +title:lung -title:fly title:rat title:bear title:insect +title:fruit)~2)",
				parse.toString());


		//set using global minimum should match
		zuliaFlexibleQueryParser.setMinimumNumberShouldMatch(2);
		zuliaFlexibleQueryParser.setDefaultFields(List.of("title"));
		String mmQuery2 = "cancer AND lung -fly rat bear insect +fruit dragon";

		zuliaFlexibleQueryParser.setDefaultOperator(ZuliaQuery.Query.Operator.OR);
		parse = zuliaFlexibleQueryParser.parse(mmQuery2);
		Assertions.assertEquals("(+title:cancer +title:lung -title:fly title:rat title:bear title:insect +title:fruit title:dragon)~2",
				parse.toString());

		zuliaFlexibleQueryParser.setDefaultOperator(ZuliaQuery.Query.Operator.AND);
		parse = zuliaFlexibleQueryParser.parse(mmQuery2);
		Assertions.assertEquals("(+title:cancer +title:lung -title:fly title:rat title:bear title:insect +title:fruit title:dragon)~2",
				parse.toString());




	}

}

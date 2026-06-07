package io.zulia.queryparser.test;

import io.zulia.DefaultAnalyzers;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.search.queryparser.ZuliaFlexibleQueryParser;
import io.zulia.server.search.queryparser.parser.ParseException;
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

	@Test
	public void offsetlessTimestampQueryTest() throws Exception {

		ZuliaFlexibleQueryParser zuliaFlexibleQueryParser = dateFieldParser();

		// timestamps are used as range bounds; an offset-less bound is assumed UTC, matching the explicit Z form
		Query offsetless = zuliaFlexibleQueryParser.parse("added:[2024-06-17T16:10:00 TO 2024-06-18T08:00:00]");
		Query explicitUtc = zuliaFlexibleQueryParser.parse("added:[2024-06-17T16:10:00Z TO 2024-06-18T08:00:00Z]");
		Assertions.assertEquals(explicitUtc, offsetless);

		// sanity: a different instant parses to a different range, so the equality above is not two empty parses
		Query differentInstant = zuliaFlexibleQueryParser.parse("added:[2024-06-17T16:11:00Z TO 2024-06-18T08:00:00Z]");
		Assertions.assertNotEquals(explicitUtc, differentInstant);
	}

	@Test
	public void quotedTimestampQueryTest() throws Exception {

		ZuliaFlexibleQueryParser zuliaFlexibleQueryParser = dateFieldParser();

		// a bare timestamp point query fails because the ':' are read as field separators
		Assertions.assertThrows(ParseException.class, () -> zuliaFlexibleQueryParser.parse("added:2024-06-17T16:10:00Z"));

		// quoting it is the escape: it parses to the same single-instant range as the explicit [x TO x] range form
		Query quoted = zuliaFlexibleQueryParser.parse("added:\"2024-06-17T16:10:00Z\"");
		Query range = zuliaFlexibleQueryParser.parse("added:[2024-06-17T16:10:00Z TO 2024-06-17T16:10:00Z]");
		Assertions.assertEquals(range, quoted);

		// backslash-escaping the colons is the other escape and yields the same query
		Query escaped = zuliaFlexibleQueryParser.parse("added:2024-06-17T16\\:10\\:00Z");
		Assertions.assertEquals(quoted, escaped);

		// quoting also accepts an offset-less timestamp (assumed UTC)
		Query quotedOffsetless = zuliaFlexibleQueryParser.parse("added:\"2024-06-17T16:10:00\"");
		Assertions.assertEquals(quoted, quotedOffsetless);
	}

	private static ZuliaFlexibleQueryParser dateFieldParser() throws Exception {
		ZuliaIndex.IndexSettings.Builder builder = ZuliaIndex.IndexSettings.newBuilder();
		builder.addFieldConfig(ZuliaIndex.FieldConfig.newBuilder().addIndexAs(ZuliaIndex.IndexAs.newBuilder().setIndexFieldName("added").build())
				.setFieldType(ZuliaIndex.FieldConfig.FieldType.DATE).setStoredFieldName("added").build());

		ServerIndexConfig serverIndexConfig = new ServerIndexConfig(builder.build());
		return new ZuliaFlexibleQueryParser(new ZuliaPerFieldAnalyzer(serverIndexConfig), serverIndexConfig);
	}

}

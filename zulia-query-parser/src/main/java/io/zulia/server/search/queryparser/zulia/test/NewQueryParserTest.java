package io.zulia.server.search.queryparser.zulia.test;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.IndexAs;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.search.queryparser.zulia.ZuliaQueryParser;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.search.Query;

import java.util.List;

public class NewQueryParserTest {

	public static void main(String[] args) throws QueryNodeException {

		ZuliaIndex.IndexSettings.Builder indexSettingBuilder = ZuliaIndex.IndexSettings.newBuilder();
		indexSettingBuilder.setIndexName("test");
		indexSettingBuilder.addFieldConfig(FieldConfig.newBuilder().setStoredFieldName("title").setFieldType(FieldType.STRING)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("title").setAnalyzerName("text").build()).build());
		indexSettingBuilder.addFieldConfig(FieldConfig.newBuilder().setStoredFieldName("pubYear").setFieldType(FieldType.DATE)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("pubYear2").build()).build());

		ServerIndexConfig serverIndexConfig = new ServerIndexConfig(indexSettingBuilder.build());
		ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer = new ZuliaPerFieldAnalyzer(serverIndexConfig);

		ZuliaQueryParser zuliaQueryParser = new ZuliaQueryParser(zuliaPerFieldAnalyzer, serverIndexConfig);

		zuliaQueryParser.setMultiFields(List.of("default1", "pubYear2"));

		try {

			Query query;
			query = zuliaQueryParser.parse("title,abstract:(\"lung cancer\" OR diabetes OR stuff)~2 AND b AND pubYear2:2015-01-01");
			System.out.println(query);

			zuliaQueryParser.setMinMatch(2);
			query = zuliaQueryParser.parse("\"ice cream\" pubYear:2000 soda cola other AND |author|:[0 TO 5]");
			System.out.println(query);

		}
		catch (QueryNodeException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}

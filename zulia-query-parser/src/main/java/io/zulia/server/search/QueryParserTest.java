package io.zulia.server.search;

import io.zulia.message.ZuliaIndex;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.config.ServerIndexConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;

public class QueryParserTest {

    public static void main(String[] args) throws ParseException {

        ZuliaIndex.IndexSettings.Builder indexSettingBuilder = ZuliaIndex.IndexSettings.newBuilder();
        indexSettingBuilder.setIndexName("test");
        indexSettingBuilder.addFieldConfig(ZuliaIndex.FieldConfig.newBuilder().setStoredFieldName("title").addIndexAs(ZuliaIndex.IndexAs.newBuilder().setIndexFieldName("title").setAnalyzerName("text").build()).build());

        ServerIndexConfig indexConfig = new ServerIndexConfig(indexSettingBuilder.build());
        ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer = new ZuliaPerFieldAnalyzer(indexConfig);

        ZuliaMultiFieldQueryParser parser = new ZuliaMultiFieldQueryParser(zuliaPerFieldAnalyzer, indexConfig);

        Query q = parser.parse("title:\"some title\"");

        Query q1 = parser.parse("|title|:1");

        System.out.println(q1);
    }
}

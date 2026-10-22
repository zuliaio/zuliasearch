package io.zulia.client;

import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.factory.Terms;
import io.zulia.client.command.factory.Values;
import io.zulia.message.ZuliaQuery;
import io.zulia.util.QueryHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class HelperTest {

	@Test
	public void valuesTest() {

		{
			String query = Values.single("fast cat").asString();
			Assertions.assertEquals("(\"fast cat\")", query);
		}

		{
			String query = Values.single("fast cat").withFields("title", "abstract").asString();
			Assertions.assertEquals("title,abstract:(\"fast cat\")", query);
		}

		{
			String query = Values.any().of("a", "b").asString();
			Assertions.assertEquals("(a OR b)", query);
		}

		{
			String query = Values.any().of("slow cat", "Pink Shirt").asString();
			Assertions.assertEquals("(\"slow cat\" OR \"Pink Shirt\")", query);
		}

		{
			String query = Values.all().of("slow cat", "Pink Shirt").asString();
			Assertions.assertEquals("(\"slow cat\" AND \"Pink Shirt\")", query);
		}

		{
			String query = Values.all().valueHandlerChain(List.of(String::toLowerCase, QueryHelper.VALUE_QUOTER)).of("slow cat", "Pink Shirt").asString();
			Assertions.assertEquals("(\"slow cat\" AND \"pink shirt\")", query);
		}

		{
			String query = Values.any().of("a", "b").withFields("title", "abstract").asString();
			Assertions.assertEquals("title,abstract:(a OR b)", query);
		}

		{
			String query = Values.any().of("a", "b", "c").withFields("title", "abstract").exclude().asString();
			Assertions.assertEquals("-title,abstract:(a OR b OR c)", query);
		}

		{
			String query = Values.atLeast(2).of("fast dog", "b", "c").withFields("title", "abstract").asString();
			Assertions.assertEquals("title,abstract:(\"fast dog\" OR b OR c)~2", query);
		}

		{
			String query = Values.atLeast(2).of("a", "b", "c").withFields("title", "abstract").exclude().asString();
			Assertions.assertEquals("-title,abstract:(a OR b OR c)~2", query);
		}

		{
			// a slash forces quoting and needs no escape inside quotes
			String query = Values.single("10.1000/jrnl.2011-0102-a").withFields("identifierExact").asString();
			Assertions.assertEquals("identifierExact:(\"10.1000/jrnl.2011-0102-a\")", query);
		}

		{
			// a backslash is doubled so the parser hands the literal value to the index
			String query = Values.single("share\\reports\\q1").withFields("identifierExact").asString();
			Assertions.assertEquals("identifierExact:(\"share\\\\reports\\\\q1\")", query);
		}

		{
			String query = Values.single("12\" monitor").withFields("identifierExact").asString();
			Assertions.assertEquals("identifierExact:(\"12\\\" monitor\")", query);
		}

		{
			FilterQuery fq = Values.atLeast(2).of("a", "b", "c").withFields("title", "abstract").exclude().asFilterQuery();
			FilterQuery fq2 = new FilterQuery("a b c").setDefaultOperator(ZuliaQuery.Query.Operator.OR).exclude().addQueryFields("title", "abstract")
					.setMinShouldMatch(2);
			Assertions.assertEquals(fq.getQuery(), fq2.getQuery());
		}

		{
			FilterQuery fq = Values.all().of("a", "b", "c").withFields("title", "abstract").exclude().asFilterQuery();
			FilterQuery fq2 = new FilterQuery("a b c").setDefaultOperator(ZuliaQuery.Query.Operator.AND).exclude().addQueryFields("title", "abstract");
			Assertions.assertEquals(fq.getQuery(), fq2.getQuery());
		}

		{
			ScoredQuery sq = Values.atLeast(2).of("a", "b", "c").withFields("title", "abstract").asScoredQuery();
			ScoredQuery sq2 = new ScoredQuery("a b c").setDefaultOperator(ZuliaQuery.Query.Operator.OR).addQueryFields("title", "abstract")
					.setMinShouldMatch(2);
			Assertions.assertEquals(sq.getQuery(), sq2.getQuery());
		}

		{
			Assertions.assertThrows(IllegalStateException.class, () -> {
				// exclude not supported in scored queries
				ScoredQuery sq = Values.atLeast(2).of("a", "b", "c").withFields("title", "abstract").exclude().asScoredQuery();
			});
		}

	}

	@Test
	public void termsTest() {

		{
			String query = Terms.withField("identifierExact").of("10.1000/jrnl.2011-0102-a", "10.1000/jrnl.2011-0345-b").asString();
			Assertions.assertEquals("identifierExact:zl:tq(\"10.1000/jrnl.2011-0102-a\" \"10.1000/jrnl.2011-0345-b\")", query);
		}

		{
			String query = Terms.withField("identifierExact").of("share\\reports\\q1").asString();
			Assertions.assertEquals("identifierExact:zl:tq(\"share\\\\reports\\\\q1\")", query);
		}

		{
			String query = Terms.withField("identifierExact").of("12\" monitor").asString();
			Assertions.assertEquals("identifierExact:zl:tq(\"12\\\" monitor\")", query);
		}

		{
			String query = Terms.withFields("id", "altId").of("a", "b c").exclude().asString();
			Assertions.assertEquals("-id,altId:zl:tq(a \"b c\")", query);
		}

	}

}

package io.zulia.client;

import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.factory.Values;
import io.zulia.message.ZuliaQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class HelperTest {

	@Test
	public void valuesTest() {

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
			String query = Values.all().valueHandlerChain(List.of(String::toLowerCase, Values.VALUE_QUOTER)).of("slow cat", "Pink Shirt").asString();
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

}

package io.zulia.server.connection.server.validation;

import io.zulia.message.ZuliaQuery.DistinctCountRequest;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.server.search.distinct.DistinctCountCollectorManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QueryRequestValidatorDistinctCountTest {

	private static final int MAX_CAP = DistinctCountCollectorManager.MAX_MAX_GROUPS_PER_SHARD;

	@Test
	public void defaultCapIsStampedIntoTheRequest() {
		QueryRequest request = QueryRequest.newBuilder().addIndex("articles").addDistinctCount(DistinctCountRequest.newBuilder().setField("journal")).build();
		QueryRequest validated = new QueryRequestValidator().validateAndSetDefault(request);
		Assertions.assertEquals(DistinctCountCollectorManager.DEFAULT_MAX_GROUPS_PER_SHARD, validated.getDistinctCount(0).getMaxGroupsPerShard());

		QueryRequest explicit = QueryRequest.newBuilder().addIndex("articles")
				.addDistinctCount(DistinctCountRequest.newBuilder().setField("journal").setMaxGroupsPerShard(5)).build();
		Assertions.assertEquals(5, new QueryRequestValidator().validateAndSetDefault(explicit).getDistinctCount(0).getMaxGroupsPerShard());
	}

	@Test
	public void badShapesAreRejectedWithTheFieldNamed() {
		QueryRequestValidator validator = new QueryRequestValidator();

		QueryRequest empty = QueryRequest.newBuilder().addIndex("articles").addDistinctCount(DistinctCountRequest.newBuilder()).build();
		Assertions.assertThrows(IllegalArgumentException.class, () -> validator.validateAndSetDefault(empty));

		QueryRequest twice = QueryRequest.newBuilder().addIndex("articles").addDistinctCount(DistinctCountRequest.newBuilder().setField("journal"))
				.addDistinctCount(DistinctCountRequest.newBuilder().setField("journal")).build();
		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> validator.validateAndSetDefault(twice));
		Assertions.assertTrue(e.getMessage().contains("<journal>") && e.getMessage().contains("more than once"), e.getMessage());

		QueryRequest negative = QueryRequest.newBuilder().addIndex("articles")
				.addDistinctCount(DistinctCountRequest.newBuilder().setField("journal").setMaxGroupsPerShard(-1)).build();
		e = Assertions.assertThrows(IllegalArgumentException.class, () -> validator.validateAndSetDefault(negative));
		Assertions.assertTrue(e.getMessage().contains("4294967295") && e.getMessage().contains("8 bytes"), e.getMessage());

		QueryRequest tooLarge = QueryRequest.newBuilder().addIndex("articles")
				.addDistinctCount(DistinctCountRequest.newBuilder().setField("journal").setMaxGroupsPerShard(MAX_CAP + 1)).build();
		e = Assertions.assertThrows(IllegalArgumentException.class, () -> validator.validateAndSetDefault(tooLarge));
		Assertions.assertTrue(e.getMessage().contains("16000001") && e.getMessage().contains("16000000"), e.getMessage());

		QueryRequest twoFieldsOverTogether = QueryRequest.newBuilder().addIndex("articles")
				.addDistinctCount(DistinctCountRequest.newBuilder().setField("journal").setMaxGroupsPerShard(9_000_000))
				.addDistinctCount(DistinctCountRequest.newBuilder().setField("year").setMaxGroupsPerShard(9_000_000)).build();
		e = Assertions.assertThrows(IllegalArgumentException.class, () -> validator.validateAndSetDefault(twoFieldsOverTogether));
		Assertions.assertTrue(e.getMessage().contains("18000000") && e.getMessage().contains("across all fields"), e.getMessage());

		QueryRequest atLimit = QueryRequest.newBuilder().addIndex("articles")
				.addDistinctCount(DistinctCountRequest.newBuilder().setField("journal").setMaxGroupsPerShard(MAX_CAP)).build();
		Assertions.assertEquals(MAX_CAP, validator.validateAndSetDefault(atLimit).getDistinctCount(0).getMaxGroupsPerShard());
	}
}

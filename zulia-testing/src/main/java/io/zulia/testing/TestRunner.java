package io.zulia.testing;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.util.exception.ThrowingSupplier;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class TestRunner {

	public static class Result {

		public Result(long count) {
			this.count = count;
		}

		public long count;
	}

	public static void main(String[] args) throws Exception {
		Yaml yaml = new Yaml();

		ZuliaTestConfig dataQualityConfig;
		try (InputStream inputStream = TestRunner.class.getResource("/testing.yaml").openStream()) {
			dataQualityConfig = yaml.loadAs(inputStream, ZuliaTestConfig.class);
		}

		Map<String, Supplier<ZuliaWorkPool>> connectionToConnectionConfig = new HashMap<>();
		for (Map.Entry<String, ConnectionConfig> indexConfigEntry : dataQualityConfig.getConnections().entrySet()) {
			ConnectionConfig connectionConfig = indexConfigEntry.getValue();

			ThrowingSupplier<ZuliaWorkPool> throwingSupplier = () -> {
				ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig().addNode(connectionConfig.getServerAddress(), connectionConfig.getPort());
				return new ZuliaWorkPool(zuliaPoolConfig);
			};

			connectionToConnectionConfig.put(indexConfigEntry.getKey(), Suppliers.memoize(throwingSupplier));
		}

		for (Map.Entry<String, IndexConfig> indexConfigEntry : dataQualityConfig.getIndexes().entrySet()) {
			IndexConfig indexConfig = indexConfigEntry.getValue();
			Supplier<ZuliaWorkPool> zuliaWorkPoolSupplier = connectionToConnectionConfig.get(indexConfig.getConnection());
			if (zuliaWorkPoolSupplier == null) {
				throw new IllegalArgumentException(
						"Failed to find connection config <" + indexConfig.getConnection() + "> for query <" + indexConfigEntry.getKey() + ">");
			}
		}

		Map<String, Long> countMap = new HashMap<>();
		for (Map.Entry<String, QueryConfig> queryConfigEntry : dataQualityConfig.getQueries().entrySet()) {
			String queryId = queryConfigEntry.getKey();
			QueryConfig queryConfig = queryConfigEntry.getValue();
			IndexConfig indexConfig = dataQualityConfig.getIndexes().get(queryConfig.getIndex());
			Supplier<ZuliaWorkPool> zuliaWorkPoolSupplier = connectionToConnectionConfig.get(indexConfig.getConnection());
			ZuliaWorkPool zuliaWorkPool = zuliaWorkPoolSupplier.get();

			Search s = new Search(indexConfig.getIndexName());
			s.addQuery(new FilterQuery(queryConfig.getQuery()));

			SearchResult searchResult = zuliaWorkPool.search(s);
			countMap.put(queryId, searchResult.getTotalHits());

		}

		try (Context context = Context.newBuilder("js").allowHostAccess(HostAccess.ALL).build()) {
			Value jsValue = context.getBindings("js");
			for (Map.Entry<String, Long> countToEntry : countMap.entrySet()) {
				jsValue.putMember(countToEntry.getKey(), new Result(countToEntry.getValue()));
			}

			for (Map.Entry<String, TestConfig> testConfigEntry : dataQualityConfig.getTests().entrySet()) {
				TestConfig testConfig = testConfigEntry.getValue();

				boolean testResult = context.eval("js", testConfig.getExpr()).asBoolean();
				if (!testResult) {
					System.out.println(testConfigEntry.getKey() + " has failed");
				}
				else {
					System.out.println(testConfigEntry.getKey() + " has passed");
				}

			}

		}

	}
}

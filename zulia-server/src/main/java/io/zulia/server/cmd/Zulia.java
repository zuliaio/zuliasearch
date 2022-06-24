package io.zulia.server.cmd;

import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.NumericStat;
import io.zulia.client.command.builder.QueryBuilder;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.command.builder.StatBuilder;
import io.zulia.client.command.builder.StatFacet;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.CompleteResult;
import io.zulia.client.result.SearchResult;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.cmd.common.MultipleIndexArgs;
import io.zulia.server.cmd.common.ShowStackArgs;
import io.zulia.server.cmd.common.ZuliaVersionProvider;
import io.zulia.server.cmd.zuliaadmin.ConnectionInfo;
import org.bson.Document;
import picocli.CommandLine;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

@CommandLine.Command(name = "zulia", subcommandsRepeatable = true, mixinStandardHelpOptions = true, versionProvider = ZuliaVersionProvider.class, scope = CommandLine.ScopeType.INHERIT, preprocessor = Zulia.MagicFinish.class)
public class Zulia {

	private static final DecimalFormat df = new DecimalFormat("#0.00");

	public static class MagicFinish implements CommandLine.IParameterPreprocessor {
		public boolean preprocess(Stack<String> args, CommandLine.Model.CommandSpec commandSpec, CommandLine.Model.ArgSpec argSpec, Map<String, Object> info) {
			if (!args.isEmpty() && !args.elementAt(0).equals("finish")) {
				args.insertElementAt("finish", 0);
			}
			return false;
		}
	}

	@CommandLine.Mixin
	private ConnectionInfo connectionInfo;

	@CommandLine.Mixin
	private ShowStackArgs showStackArgs;

	@CommandLine.Mixin
	private MultipleIndexArgs multipleIndexArgs;

	private final List<QueryBuilder> queryBuilders = new ArrayList<>();

	private final List<CountFacet> countFacets = new ArrayList<>();

	private final List<StatBuilder> stats = new ArrayList<>();

	private final List<Sort> sorts = new ArrayList<>();

	@CommandLine.Option(names = { "-r", "--rows" }, description = "Number of rows to return", scope = CommandLine.ScopeType.INHERIT)
	private Integer rows;

	@CommandLine.Option(names = {
			"--start" }, description = "Starting index in results (use for simple paging) but avoid large values", scope = CommandLine.ScopeType.INHERIT)
	private Integer start;

	@CommandLine.Option(names = {
			"--fetch" }, description = "Fetch type whether the full record is fetched or just Id or user defined meta data  (FULL, META, NONE, default: ${DEFAULT-VALUE}) ", scope = CommandLine.ScopeType.INHERIT)
	private final ZuliaQuery.FetchType fetch = ZuliaQuery.FetchType.NONE;

	@CommandLine.Option(names = "--fl", description = "List of fields to return", scope = CommandLine.ScopeType.INHERIT)
	private Set<String> fl;

	@CommandLine.Option(names = "--flMask", description = "List of fields to mask", scope = CommandLine.ScopeType.INHERIT)
	private Set<String> flMask;

	@CommandLine.Command(aliases = { "sq", "scoredQuery" })
	void search(@CommandLine.Option(names = { "-q", "--query" }, required = true) String query,
			@CommandLine.Option(names = { "--qf", "--queryFields" }, description = "Fields to search for terms in the query without an explicit field given")
			List<String> queryFields,
			@CommandLine.Option(names = { "-m", "--mm", "--minimumShouldMatch" }, description = "How many optional (ORed) terms are required") Integer minMatch,
			@CommandLine.Option(names = { "-o", "--defaultOperator" }, description = "The default operator to use if not explicitly defined between terms")
			ZuliaQuery.Query.Operator defaultOperator) {

		ScoredQuery scoredQuery = new ScoredQuery(query);
		if (queryFields != null) {
			scoredQuery.addQueryFields(queryFields);
		}
		if (minMatch != null) {
			scoredQuery.setMinShouldMatch(minMatch);
		}
		if (defaultOperator != null) {
			scoredQuery.setDefaultOperator(defaultOperator);
		}
		queryBuilders.add(scoredQuery);

	}

	@CommandLine.Command(aliases = { "fq", "filterQuery" })
	void filter(@CommandLine.Option(names = { "-q", "--query" }, required = true) String query,
			@CommandLine.Option(names = { "--qf", "--queryFields" }, description = "Fields to search for terms in the query without an explicit field given")
			List<String> queryFields,
			@CommandLine.Option(names = { "-m", "--mm", "--minimumShouldMatch" }, description = "How many optional (ORed) terms are required") Integer minMatch,
			@CommandLine.Option(names = { "-o", "--defaultOperator" }, description = "The default operator to use if not explicitly defined between terms")
			ZuliaQuery.Query.Operator defaultOperator) {

		FilterQuery filterQuery = new FilterQuery(query);
		if (queryFields != null) {
			filterQuery.addQueryFields(queryFields);
		}
		if (minMatch != null) {
			filterQuery.setMinShouldMatch(minMatch);
		}
		if (defaultOperator != null) {
			filterQuery.setDefaultOperator(defaultOperator);
		}

		queryBuilders.add(filterQuery);
	}

	@CommandLine.Command(aliases = { "c", "count" })
	void facet(@CommandLine.Option(names = { "-f", "--facetField" }, required = true, description = "Facet facet field name") String facetField,
			@CommandLine.Option(names = { "-p", "--path" }, description = "Path values for a hierarchical facet") List<String> path,
			@CommandLine.Option(names = { "-t", "--topN" }, description = "The number of facets to return") Integer topN, @CommandLine.Option(names = { "-s",
			"--shardTopN" }, description = "The number of facets to request from each shard.  Increasing this number can increase the accuracy of sharded facets when all of the facets are not returned in the top N")
	Integer shardTopN) {
		CountFacet countFacet = path != null && !path.isEmpty() ? new CountFacet(facetField, path) : new CountFacet(facetField);

		if (topN != null) {
			countFacet.setTopN(topN);
		}
		if (shardTopN != null) {
			countFacet.setTopNShard(shardTopN);
		}
		countFacets.add(countFacet);

	}

	@CommandLine.Command(aliases = { "sf" })
	void statFacet(@CommandLine.Option(names = { "-n", "--numericField" }, required = true, description = "Numeric field name") String numericField,
			@CommandLine.Option(names = { "-f", "--facetField" }, required = true, description = "Facet field name") String facetField,
			@CommandLine.Option(names = { "-t", "--topN" }, description = "The number of facets to return") Integer topN, @CommandLine.Option(names = { "-s",
			"--shardTopN" }, description = "The number of facets to request from each shard.  Increasing this number can increase the accuracy of sharded facets when all of the facets are not returned in the top N")
	Integer shardTopN, @CommandLine.Option(names = { "-p", "--path" }, description = "Path values for a hierarchical facet") List<String> path) {

		StatFacet statFacet = path != null && !path.isEmpty() ? new StatFacet(numericField, facetField, path) : new StatFacet(numericField, facetField);
		if (topN != null) {
			statFacet.setTopN(topN);
		}
		if (shardTopN != null) {
			statFacet.setTopNShard(shardTopN);
		}
		stats.add(statFacet);
	}

	@CommandLine.Command(aliases = { "st" })
	void stat(@CommandLine.Option(names = { "-n", "--numericField" }, required = true, description = "Numeric field name") String numericField) {
		NumericStat numericStat = new NumericStat(numericField);
		stats.add(numericStat);
	}

	@CommandLine.Command(aliases = { "so" })
	void sort(@CommandLine.Option(names = { "-s", "--sortField" }, required = true, description = "Sort field name") String sortField,
			@CommandLine.Option(names = { "-o", "--order" }, required = true, description = "Order") ZuliaQuery.FieldSort.Direction order) {
		Sort sort = new Sort(sortField);
		if (order != null) {
			if (ZuliaQuery.FieldSort.Direction.ASCENDING.equals(order)) {
				sort.ascending();
			}
			else if (ZuliaQuery.FieldSort.Direction.DESCENDING.equals(order)) {
				sort.descending();
			}
		}
		sorts.add(sort);
	}

	@CommandLine.Command(hidden = true)
	int finish() throws Exception {
		ZuliaWorkPool zuliaWorkPool = connectionInfo.getConnection();
		Search search = new Search(multipleIndexArgs.resolveIndexes(zuliaWorkPool));
		for (QueryBuilder queryBuilder : queryBuilders) {
			search.addQuery(queryBuilder);
		}

		for (CountFacet countFacet : countFacets) {
			search.addCountFacet(countFacet);
		}

		for (StatBuilder stat : stats) {
			search.addStat(stat);
		}

		for (Sort sort : sorts) {
			search.addSort(sort);
		}

		if (rows != null) {
			search.setAmount(rows);
		}
		if (start != null) {
			search.setStart(start);
		}
		if (fetch != null) {
			search.setResultFetchType(fetch);
		}

		if (fl != null) {
			for (String f : fl) {
				search.addDocumentField(f);
			}
		}

		if (flMask != null) {
			for (String f : flMask) {
				search.addDocumentMaskedField(f);
			}
		}

		SearchResult searchResult = zuliaWorkPool.search(search);
		display(searchResult);

		return CommandLine.ExitCode.OK;
	}

	public void display(SearchResult searchResult) {
		List<CompleteResult> completeResults = searchResult.getCompleteResults();

		ZuliaCommonCmd.printBlue("QueryTime: " + searchResult.getCommandTimeMs() + "ms");
		ZuliaCommonCmd.printBlue("TotalResults: " + searchResult.getTotalHits());
		System.out.println();

		if (rows != null && rows != 0) {

			ZuliaCommonCmd.printBlue("Results:");

			if (ZuliaQuery.FetchType.NONE.equals(fetch)) {
				ZuliaCommonCmd.printMagenta(String.format("%25s | %40s | %10s | %10s", "UniqueId", "Index", "Score", "Shard #"));
			}
			else if (ZuliaQuery.FetchType.META.equals(fetch)) {
				ZuliaCommonCmd.printMagenta(String.format("%25s | %40s | %10s | %10s | %60s", "UniqueId", "Index", "Score", "Shard #", "Meta"));
			}
			else if (ZuliaQuery.FetchType.FULL.equals(fetch)) {
				ZuliaCommonCmd.printMagenta(String.format("%25s | %40s | %10s | %10s | %60s", "UniqueId", "Index", "Score", "Shard #", "Document"));
			}

			for (CompleteResult completeResult : completeResults) {
				System.out.printf("%25s | %40s | %10s | %10s", completeResult.getUniqueId(), completeResult.getIndexName(),
						df.format(completeResult.getScore()), completeResult.getShard());

				if (ZuliaQuery.FetchType.META.equals(fetch)) {
					Document mongoDocument = completeResult.getMetadata();
					System.out.printf(" | %60s", mongoDocument == null ? "" : mongoDocument.toJson());

				}
				if (ZuliaQuery.FetchType.FULL.equals(fetch)) {
					Document mongoDocument = completeResult.getDocument();
					System.out.printf(" | %60s", mongoDocument == null ? "" : mongoDocument.toJson());
				}

				System.out.println();
			}
			System.out.println();
		}

		if (!searchResult.getFacetGroups().isEmpty()) {

			ZuliaCommonCmd.printBlue("Count Facets:");
			ZuliaCommonCmd.printMagenta(String.format("%25s | %25s | %12s | %12s", "Field", "Label", "Count", "Max Error"));
			for (ZuliaQuery.FacetGroup fg : searchResult.getFacetGroups()) {
				StringBuilder label = new StringBuilder(fg.getCountRequest().getFacetField().getLabel());
				for (String path : fg.getCountRequest().getFacetField().getPathList()) {
					label.append(label).append("/").append(path);
				}
				for (ZuliaQuery.FacetCount fc : fg.getFacetCountList()) {
					System.out.printf("%25s | %25s | %12s | %12s", label, fc.getFacet(), fc.getCount(), fc.getMaxError());
					System.out.println();
				}
			}
			System.out.println();
		}

		if (!searchResult.getFacetFieldStats().isEmpty()) {

			ZuliaCommonCmd.printBlue("Stat Facets:");
			ZuliaCommonCmd.printMagenta(
					String.format("%25s | %15s | %15s | %25s | %12s | %12s | %12s | %12s", "Numeric Field", "Facet Field", "Facet Label", "Value Count",
							"Doc Count", "Min", "Max", "Sum"));
			for (ZuliaQuery.StatGroup sg : searchResult.getFacetFieldStats()) {
				StringBuilder label = new StringBuilder(sg.getStatRequest().getFacetField().getLabel());
				for (String path : sg.getStatRequest().getFacetField().getPathList()) {
					label.append(label).append("/").append(path);
				}

				for (ZuliaQuery.FacetStats fs : sg.getFacetStatsList()) {
					System.out.printf("%25s | %15s | %15s | %25s | %12s | %12s | %12s | %12s", sg.getStatRequest().getNumericField(), label, fs.getFacet(),
							fs.getValueCount(), fs.getDocCount(), getVal(fs.getMin()), getVal(fs.getMax()), getVal(fs.getSum()));
					System.out.println();
				}
			}
			System.out.println();
		}

		if (!searchResult.getNumericFieldStats().isEmpty()) {

			ZuliaCommonCmd.printBlue("Numeric Stats:");
			ZuliaCommonCmd.printMagenta(String.format("%25s | %12s | %12s | %12s | %12s | %12s", "Field", "Value Count", "Doc Count", "Min", "Max", "Sum"));
			for (ZuliaQuery.StatGroup sg : searchResult.getNumericFieldStats()) {
				ZuliaQuery.FacetStats globalStats = sg.getGlobalStats();

				System.out.printf("%25s | %12s | %12s | %12s | %12s | %12s", sg.getStatRequest().getNumericField(), globalStats.getValueCount(),
						globalStats.getDocCount(), getVal(globalStats.getMin()), getVal(globalStats.getMax()), getVal(globalStats.getSum()));
				System.out.println();
			}
			System.out.println();
		}
	}

	private String getVal(ZuliaQuery.SortValue val) {
		return val.getLongValue() != 0 ? val.getLongValue() + "" : df.format(val.getDoubleValue());
	}

	public static void main(String[] args) {
		ZuliaCommonCmd.runCommandLine(new Zulia(), args);
	}

}

package io.zulia.server.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import io.zulia.client.command.ClearIndex;
import io.zulia.client.command.DeleteIndex;
import io.zulia.client.command.GetIndexes;
import io.zulia.client.command.GetNodes;
import io.zulia.client.command.GetNumberOfDocs;
import io.zulia.client.command.OptimizeIndex;
import io.zulia.client.command.Reindex;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.ClearIndexResult;
import io.zulia.client.result.DeleteIndexResult;
import io.zulia.client.result.GetFieldsResult;
import io.zulia.client.result.GetIndexesResult;
import io.zulia.client.result.GetNodesResult;
import io.zulia.client.result.GetNumberOfDocsResult;
import io.zulia.client.result.OptimizeIndexResult;
import io.zulia.client.result.ReindexResult;
import io.zulia.client.result.SearchResult;
import io.zulia.log.LogUtil;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaQuery;
import io.zulia.util.ZuliaUtil;
import io.zulia.util.ZuliaVersion;
import org.bson.Document;

import java.text.DecimalFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class Zulia {

	private static final DecimalFormat df = new DecimalFormat("#.00");

	@Parameters(commandNames = "query", commandDescription = "Queries the given index in --index argument.")
	public static class QueryCmd {

		@Parameter(names = "--indexes", description = "Indexes to query, none to default to the required argument or many.")
		private Set<String> indexes;

		@Parameter(names = "--q", description = "Zulia query, matches all docs by default.")
		private String q;

		@Parameter(names = "--rows", description = "Number of records to return.")
		private Integer rows = 0;

		@Parameter(names = "--start", description = "Results start index.")
		private Integer start = 0;

		@Parameter(names = "--facets", description = "List of fields to facet on.")
		private Set<String> facets;

		@Parameter(names = "--facetCount", description = "Number of facets to return.")
		private Integer facetCount = 10;

		@Parameter(names = "--facetShardCount", description = "Number of facets to return per shard.")
		private Integer facetShardCount = 40;

		@Parameter(names = "--sort", description = "List of fields to sort on.")
		private Set<String> sortFields;

		@Parameter(names = "--sortDesc", description = "List of fields to sort on in descending order.")
		private Set<String> sortDescFields;

		@Parameter(names = "--qf", description = "Specific field(s) to search, index default if none given.")
		private Set<String> qf;

		@Parameter(names = "--fq", description = "Filter query.")
		private Set<String> fq;

		@Parameter(names = "--minimumNumberShouldMatch", description = "Minimum number of optional boolean queries to match")
		private Integer minimumNumberShouldMatch;

		@Parameter(names = "--fetch", description = "Fetch type (none, metadata, full)")
		private String fetch = "none";

		@Parameter(names = "--fl", description = "List of fields to return")
		private Set<String> fl;

		@Parameter(names = "--flMask", description = "List of fields to mask")
		private Set<String> flMask;

	}

	@Parameters(commandNames = "getIndexes", commandDescription = "Gets all available indexes.")
	public static class GetIndexesCmd {
	}

	@Parameters(commandNames = "--version", commandDescription = "Gets the Zulia version")
	public static class VersionCmd {
	}

	@Parameters(commandNames = "clear", commandDescription = "Clears the given index in --index argument.")
	public static class ClearCmd {
	}

	@Parameters(commandNames = "optimize", commandDescription = "Optimizes the given index in --index argument.")
	public static class OptimizeCmd {
	}

	@Parameters(commandNames = "getCount", commandDescription = "Gets total number of docs in the given index in --index argument.")
	public static class GetCountCmd {
	}

	@Parameters(commandNames = "getFields", commandDescription = "Gets all the fields in the given index in --index argument.")
	public static class GetFieldsCmd {
	}

	@Parameters(commandNames = "getCurrentNodes", commandDescription = "Gets the current nodes that belong to the given index in --index argument.")
	public static class GetCurrentNodesCmd {
	}

	@Parameters(commandNames = "delete", commandDescription = "Deletes the given index in --index argument.")
	public static class DeleteCmd {
	}

	@Parameters(commandNames = "reindex", commandDescription = "Reindexes the given index in --index argument.")
	public static class ReindexCmd {
	}

	public static void main(String[] args) {

		LogUtil.init();

		ZuliaBaseArgs zuliaArgs = new ZuliaBaseArgs();
		GetIndexesCmd getIndexesCmd = new GetIndexesCmd();
		VersionCmd version = new VersionCmd();
		ClearCmd clear = new ClearCmd();
		OptimizeCmd optimize = new OptimizeCmd();
		GetCountCmd getCount = new GetCountCmd();
		GetCurrentNodesCmd getCurrentNodes = new GetCurrentNodesCmd();
		GetFieldsCmd getFields = new GetFieldsCmd();
		DeleteCmd delete = new DeleteCmd();
		ReindexCmd reindex = new ReindexCmd();
		QueryCmd query = new QueryCmd();

		JCommander jCommander = JCommander.newBuilder().addObject(zuliaArgs).addCommand(version).addCommand(getIndexesCmd).addCommand(query).addCommand(clear).addCommand(getCount)
				.addCommand(getCurrentNodes).addCommand(getFields).addCommand(delete).addCommand(reindex).addCommand(optimize).build();
		try {

			jCommander.parse(args);

			if (jCommander.getParsedCommand() == null) {
				jCommander.usage();
				System.exit(2);
			}

			ZuliaPoolConfig config = new ZuliaPoolConfig().addNode(zuliaArgs.address, zuliaArgs.port);
			ZuliaWorkPool workPool = new ZuliaWorkPool(config);

			if ("--version".equalsIgnoreCase(jCommander.getParsedCommand())) {
				System.out.println(ZuliaVersion.getVersion());
				System.exit(0);
			}

			if ("getIndexes".equalsIgnoreCase(jCommander.getParsedCommand())) {
				GetIndexes getIndexes = new GetIndexes();
				GetIndexesResult execute = workPool.execute(getIndexes);
				System.out.println(execute.getIndexNames());
				System.exit(0);
			}

			String index = zuliaArgs.index;

			if (index == null) {
				System.err.println("Please pass in an index.");
				jCommander.usage();
				System.exit(2);
			}

			if ("query".equals(jCommander.getParsedCommand())) {

				Search search;
				if (query.indexes != null) {
					search = new Search(query.indexes);
				}
				else {
					search = new Search(index);
				}

				search.setAmount(query.rows);

				ScoredQuery scoredQuery = new ScoredQuery(query.q);

				if (query.qf != null) {
					query.qf.forEach(scoredQuery::addQueryField);
				}

				if (query.minimumNumberShouldMatch != null) {
					scoredQuery.setMinShouldMatch(query.minimumNumberShouldMatch);
				}

				search.addQuery(scoredQuery);

				search.setStart(query.start);

				if (query.fetch.equalsIgnoreCase("full")) {
					search.setResultFetchType(ZuliaQuery.FetchType.FULL);
				}

				if (query.facets != null) {
					for (String facet : query.facets) {
						search.addCountFacet(new CountFacet(facet).setTopN(query.facetCount).setTopNShard(query.facetShardCount));
					}
				}

				if (query.sortFields != null) {
					for (String sortField : query.sortFields) {
						search.addSort(new Sort(sortField));
					}
					;
				}

				if (query.sortDescFields != null) {
					for (String sortDesc : query.sortDescFields) {
						search.addSort(new Sort(sortDesc).descending());
					}
				}

				if (query.fq != null) {
					for (String filterQuery : query.fq) {
						search.addQuery(new FilterQuery(filterQuery));
					}
				}

				if (query.fl != null) {
					query.fl.forEach(search::addDocumentField);
				}

				if (query.flMask != null) {
					query.flMask.forEach(search::addDocumentMaskedField);
				}

				SearchResult searchResult = workPool.search(search);

				List<ZuliaQuery.ScoredResult> srList = searchResult.getResults();

				System.out.println("QueryTime: " + (searchResult.getCommandTimeMs()) + "ms");
				System.out.println("TotalResults: " + searchResult.getTotalHits());

				System.out.println("Results:");

				System.out.print("UniqueId");
				System.out.print("\t");
				System.out.print("Score");
				System.out.print("\t");
				System.out.print("Index");
				System.out.print("\t");
				System.out.print("Shard");
				System.out.print("\t");
				System.out.print("LuceneShardId");
				System.out.print("\t");
				System.out.print("Sort");
				System.out.print("\t");
				if (query.fetch.equalsIgnoreCase("full")) {
					System.out.print("Document");
				}
				System.out.println();

				for (ZuliaQuery.ScoredResult sr : srList) {
					System.out.print(sr.getUniqueId());
					System.out.print("\t");
					System.out.print(df.format(sr.getScore()));
					System.out.print("\t");
					System.out.print(sr.getIndexName());
					System.out.print("\t");
					System.out.print(sr.getShard());
					System.out.print("\t");
					System.out.print(sr.getLuceneShardId());
					System.out.print("\t");

					StringBuffer sb = new StringBuffer();

					if (sr.hasSortValues()) {
						for (ZuliaQuery.SortValue sortValue : sr.getSortValues().getSortValueList()) {
							if (sb.length() != 0) {
								sb.append(",");
							}
							if (sortValue.getExists()) {
								if (sortValue.getDateValue() != 0) {
									sb.append(new Date(sortValue.getDateValue()));
								}
								else if (sortValue.getDoubleValue() != 0) {
									sb.append(sortValue.getDoubleValue());
								}
								else if (sortValue.getFloatValue() != 0) {
									sb.append(sortValue.getFloatValue());
								}
								else if (sortValue.getIntegerValue() != 0) {
									sb.append(sortValue.getIntegerValue());
								}
								else if (sortValue.getLongValue() != 0) {
									sb.append(sortValue.getLongValue());
								}
								else if (sortValue.getStringValue() != null) {
									sb.append(sortValue.getStringValue());
								}
							}
							else {
								sb.append("!NULL!");
							}
						}
					}

					if (sb.length() != 0) {
						System.out.print(sb);
					}
					else {
						System.out.print("--");
					}

					if (query.fetch != null && query.fetch.equalsIgnoreCase("full")) {
						System.out.print("\t");
						if (sr.hasResultDocument()) {
							ZuliaBase.ResultDocument resultDocument = sr.getResultDocument();
							if (resultDocument.getDocument() != null) {
								Document mongoDocument = new Document();
								mongoDocument.putAll(ZuliaUtil.byteArrayToMongoDocument(resultDocument.getDocument().toByteArray()));
								System.out.println(mongoDocument.toJson());
							}
						}
					}

					System.out.println();
				}

				if (!searchResult.getFacetGroups().isEmpty()) {
					System.out.println("Facets:");
					for (ZuliaQuery.FacetGroup fg : searchResult.getFacetGroups()) {
						System.out.println();
						System.out.println("--Facet on " + fg.getCountRequest().getFacetField().getLabel() + "--");
						for (ZuliaQuery.FacetCount fc : fg.getFacetCountList()) {
							System.out.print(fc.getFacet());
							System.out.print("\t");
							System.out.print(fc.getCount());
							System.out.print("\t");
							System.out.print("+" + fc.getMaxError());
							System.out.println();
						}
						if (fg.getPossibleMissing()) {
							System.out.println(
									"Possible facets missing from top results for <" + fg.getCountRequest().getFacetField().getLabel() + "> with max count <"
											+ fg.getMaxValuePossibleMissing() + ">");
						}
					}

				}

			}
			else if ("clear".equals(jCommander.getParsedCommand())) {
				System.out.println("Clearing index: " + index);
				ClearIndexResult response = workPool.execute(new ClearIndex(index));
				System.out.println("Cleared index: " + index);
			}
			else if ("getCount".equals(jCommander.getParsedCommand())) {
				GetNumberOfDocsResult response = workPool.execute(new GetNumberOfDocs(index));
				System.out.println("Shards: " + response.getShardCountResponseCount());
				System.out.println("Count: " + response.getNumberOfDocs());
				for (ZuliaBase.ShardCountResponse scr : response.getShardCountResponses()) {
					System.out.println("Shard [" + scr.getShardNumber() + "] Count:\n" + scr.getNumberOfDocs());
				}
			}
			else if ("getCurrentNodes".equals(jCommander.getParsedCommand())) {
				GetNodesResult response = workPool.execute(new GetNodes());

				System.out.println("serverAddress\tservicePort\theartBeat\trestPort\tversion");
				for (ZuliaBase.Node val : response.getNodes()) {
					String nodeVersion = val.getVersion();
					if (nodeVersion == null || nodeVersion.isEmpty()) {
						nodeVersion = "< " + ZuliaVersion.getVersionAdded();
					}
					System.out.println(val.getServerAddress() + "\t" + val.getServicePort() + "\t" + val.getHeartbeat() + "\t" + val.getRestPort() + "\t" + nodeVersion);
				}
			}
			else if ("getFields".equals(jCommander.getParsedCommand())) {
				GetFieldsResult response = workPool.execute(new io.zulia.client.command.GetFields(index));
				response.getFieldNames().forEach(System.out::println);
			}
			else if ("delete".equals(jCommander.getParsedCommand())) {
				System.out.println("Deleting index: " + index);
				DeleteIndexResult response = workPool.execute(new DeleteIndex(index));
				System.out.println("Deleted index: " + index);
			}
			else if ("reindex".equals(jCommander.getParsedCommand())) {
				if (index.contains("*")) {
					GetIndexesResult indexesResult = workPool.getIndexes();
					for (String ind : indexesResult.getIndexNames()) {
						if (ind.startsWith(index.replace("*", ""))) {
							System.out.println("Reindexing index: " + ind);
							ReindexResult response = workPool.execute(new Reindex(ind));
							System.out.println("Reindexed index: " + ind);
						}
					}
				}
				else {
					System.out.println("Reindexing index: " + index);
					ReindexResult response = workPool.execute(new Reindex(index));
					System.out.println("Reindexed index: " + index);
				}

			}
			else if ("optimize".equals(jCommander.getParsedCommand())) {
				System.out.println("Optimizing index: " + index);
				OptimizeIndexResult response = workPool.execute(new OptimizeIndex(index));
				System.out.println("Optimized index: " + index);
			}

		}
		catch (Exception e) {
			if (e instanceof ParameterException) {
				System.err.println(e.getMessage());
				jCommander.usage();
				System.exit(2);
			}
			else {
				e.printStackTrace();
			}
		}
	}
}

package io.zulia.server.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import io.zulia.client.command.ClearIndex;
import io.zulia.client.command.DeleteIndex;
import io.zulia.client.command.GetNodes;
import io.zulia.client.command.GetNumberOfDocs;
import io.zulia.client.command.OptimizeIndex;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.ClearIndexResult;
import io.zulia.client.result.DeleteIndexResult;
import io.zulia.client.result.GetFieldsResult;
import io.zulia.client.result.GetNodesResult;
import io.zulia.client.result.GetNumberOfDocsResult;
import io.zulia.client.result.OptimizeIndexResult;
import io.zulia.client.result.QueryResult;
import io.zulia.log.LogUtil;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.FieldSort.Direction;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;

import java.text.DecimalFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class Zulia {

	private static final DecimalFormat df = new DecimalFormat("#.00");

	public static class ZuliaArgs {

		@Parameter(names = "--help", help = true)
		private boolean help;

		@Parameter(names = "--address", description = "Zulia Server Address", order = 1)
		private String address = "localhost";

		@Parameter(names = "--port", description = "Zulia Port", order = 2)
		private Integer port = 32191;

		@Parameter(names = "--getIndexes", description = "Gets all available indexes.", order = 3)
		private String getIndexes;

		@Parameter(names = "--index", description = "Index name", required = true, order = 4)
		private String index;

	}

	@Parameters(commandNames = "query", commandDescription = "Queries the given index in --index argument.")
	public static class Query {

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

	@Parameters(commandNames = "clear", commandDescription = "Clears the given index in --index argument.")
	public static class Clear {
	}

	@Parameters(commandNames = "optimize", commandDescription = "Optimizes the given index in --index argument.")
	public static class Optimize {
	}

	@Parameters(commandNames = "getCount", commandDescription = "Gets total number of docs in the given index in --index argument.")
	public static class GetCount {
	}

	@Parameters(commandNames = "getFields", commandDescription = "Gets all the fields in the given index in --index argument.")
	public static class GetFields {
	}

	@Parameters(commandNames = "getCurrentNodes", commandDescription = "Gets the current nodes that belong to the given index in --index argument.")
	public static class GetCurrentNodes {
	}

	@Parameters(commandNames = "delete", commandDescription = "Deletes the given index in --index argument.")
	public static class Delete {
	}

	public static void main(String[] args) {

		LogUtil.init();

		ZuliaArgs zuliaArgs = new ZuliaArgs();
		Clear clear = new Clear();
		Optimize optimize = new Optimize();
		GetCount getCount = new GetCount();
		GetCurrentNodes getCurrentNodes = new GetCurrentNodes();
		GetFields getFields = new GetFields();
		Delete delete = new Delete();
		Query query = new Query();

		JCommander jCommander = JCommander.newBuilder().addObject(zuliaArgs).addCommand(query).addCommand(clear).addCommand(getCount)
				.addCommand(getCurrentNodes).addCommand(getFields).addCommand(delete).addCommand(optimize).build();
		try {
			jCommander.parse(args);

			if (jCommander.getParsedCommand() == null) {
				jCommander.usage();
				System.exit(2);
			}

			String index = zuliaArgs.index;

			ZuliaPoolConfig config = new ZuliaPoolConfig().addNode(zuliaArgs.address, zuliaArgs.port);
			ZuliaWorkPool workPool = new ZuliaWorkPool(config);

			if ("query".equals(jCommander.getParsedCommand())) {

				if (query.q != null) {

					io.zulia.client.command.Query zuliaQuery;
					if (query.indexes != null) {
						zuliaQuery = new io.zulia.client.command.Query(query.indexes, query.q, query.rows);
					}
					else {
						zuliaQuery = new io.zulia.client.command.Query(index, query.q, query.rows);
					}

					if (query.qf != null) {
						query.qf.forEach(zuliaQuery::addQueryField);
					}

					zuliaQuery.setStart(query.start);

					if (query.fetch.equalsIgnoreCase("full")) {
						zuliaQuery.setResultFetchType(ZuliaQuery.FetchType.FULL);
					}

					if (query.minimumNumberShouldMatch != null) {
						zuliaQuery.setMinimumNumberShouldMatch(query.minimumNumberShouldMatch);
					}

					if (query.facets != null) {
						for (String facet : query.facets) {
							zuliaQuery.addCountRequest(facet, query.facetCount, query.facetShardCount);
						}
					}

					if (query.sortFields != null) {
						query.sortFields.forEach(zuliaQuery::addFieldSort);
					}

					if (query.sortDescFields != null) {
						for (String sortDesc : query.sortDescFields) {
							zuliaQuery.addFieldSort(sortDesc, Direction.DESCENDING);
						}
					}

					if (query.fq != null) {
						query.fq.forEach(zuliaQuery::addFilterQuery);
					}

					if (query.fl != null) {
						query.fl.forEach(zuliaQuery::addDocumentField);
					}

					if (query.flMask != null) {
						query.flMask.forEach(zuliaQuery::addDocumentMaskedField);
					}

					QueryResult qr = workPool.execute(zuliaQuery);

					List<ZuliaQuery.ScoredResult> srList = qr.getResults();

					System.out.println("QueryTime: " + (qr.getCommandTimeMs()) + "ms");
					System.out.println("TotalResults: " + qr.getTotalHits());

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

					if (!qr.getFacetGroups().isEmpty()) {
						System.out.println("Facets:");
						for (ZuliaQuery.FacetGroup fg : qr.getFacetGroups()) {
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
								System.out.println("Possible facets missing from top results for <" + fg.getCountRequest().getFacetField().getLabel()
										+ "> with max count <" + fg.getMaxValuePossibleMissing() + ">");
							}
						}

					}

				}
				else {
					jCommander.usage();
					System.exit(2);
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

				System.out.println("serverAddress\tservicePort\theartBeat\trestPort");
				for (ZuliaBase.Node val : response.getNodes()) {
					System.out.println(val.getServerAddress() + "\t" + val.getServicePort() + "\t" + val.getHeartbeat() + "\t" + val.getRestPort());
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

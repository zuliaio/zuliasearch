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
import io.zulia.log.LogUtil;
import io.zulia.message.ZuliaBase;

public class Zulia {

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
				// TODO: need to handle everything that goes with query
				// needs to have its own arguments
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

package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.command.GetFields;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.GetFieldsResult;
import io.zulia.tools.cmd.ZuliaAdmin;
import io.zulia.tools.cmd.common.SingleIndexArgs;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "getFields", description = "Lists all field names for an index")
public class GetFieldsCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Mixin
	private SingleIndexArgs singleIndexArgs;

	@CommandLine.Option(names = "--realtime", description = "Use realtime field list (includes uncommitted changes)")
	private boolean realtime;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		String index = singleIndexArgs.getIndex();

		GetFields getFields = new GetFields(index);
		if (realtime) {
			getFields.setRealtime(true);
		}

		GetFieldsResult result = zuliaWorkPool.getFields(getFields);

		List<String> fieldNames = result.getFieldNames();

		if (fieldNames.isEmpty()) {
			System.out.println("No fields found for index <" + index + ">");
		}
		else {
			for (String fieldName : fieldNames) {
				System.out.println(fieldName);
			}
		}

		return CommandLine.ExitCode.OK;
	}
}

package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.command.Fetch;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.FetchResult;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.tools.cmd.ZuliaAdmin;
import io.zulia.tools.cmd.common.SingleIndexArgs;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;
import picocli.CommandLine;

import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "fetchDocument", aliases = "fetchDoc", description = "Fetches a document by unique id from an index")
public class FetchDocumentCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Mixin
	private SingleIndexArgs singleIndexArgs;

	@CommandLine.Option(names = "--id", description = "Unique id of the document to fetch", required = true)
	private String id;

	@CommandLine.Option(names = "--pretty", description = "Pretty print the JSON output")
	private boolean pretty;

	@CommandLine.Option(names = "--fl", description = "Fields to return (whitelist)", split = ",")
	private List<String> fl;

	@CommandLine.Option(names = "--flMask", description = "Fields to mask (blacklist)", split = ",")
	private List<String> flMask;

	@CommandLine.Option(names = "--meta", description = "Also print document metadata")
	private boolean meta;

	@CommandLine.Option(names = { "-o", "--output" }, description = "Output file to write JSON to")
	private File output;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		String index = singleIndexArgs.getIndex();

		Fetch fetch = new Fetch(id, index);
		fetch.setResultFetchType(FetchType.FULL);

		if (fl != null) {
			for (String field : fl) {
				fetch.addDocumentField(field);
			}
		}

		if (flMask != null) {
			for (String field : flMask) {
				fetch.addDocumentMaskedField(field);
			}
		}

		FetchResult fetchResult = zuliaWorkPool.fetch(fetch);

		if (!fetchResult.hasResultDocument()) {
			System.err.println("Document with id <" + id + "> not found in index <" + index + ">");
			return CommandLine.ExitCode.SOFTWARE;
		}

		PrintStream out = System.out;
		if (output != null) {
			out = new PrintStream(output, StandardCharsets.UTF_8);
		}

		try {
			if (meta) {
				Document metaDoc = fetchResult.getMeta();
				if (metaDoc != null) {
					out.println("Metadata: " + ZuliaUtil.mongoDocumentToJson(metaDoc, pretty));
				}
			}

			if (pretty) {
				out.println(fetchResult.getDocumentAsPrettyJson());
			}
			else {
				out.println(fetchResult.getDocumentAsJson());
			}
		}
		finally {
			if (output != null) {
				out.close();
			}
		}

		return CommandLine.ExitCode.OK;
	}
}

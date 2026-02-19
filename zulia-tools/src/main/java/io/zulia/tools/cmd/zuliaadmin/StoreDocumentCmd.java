package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.command.Store;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.tools.cmd.ZuliaAdmin;
import io.zulia.tools.cmd.common.SingleIndexArgs;
import org.bson.Document;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(name = "storeDocument", aliases = "storeDoc", description = "Stores a JSON document into an index")
public class StoreDocumentCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Mixin
	private SingleIndexArgs singleIndexArgs;

	@CommandLine.Option(names = "--id", description = "Unique id for the document (extracted from document if not provided)")
	private String id;

	@CommandLine.Option(names = "--json", description = "JSON string to store")
	private String json;

	@CommandLine.Option(names = "--file", description = "Path to JSON file to read")
	private File file;

	@CommandLine.Option(names = "--stdin", description = "Read JSON from stdin")
	private boolean stdin;

	@CommandLine.Option(names = "--idField", description = "Field name to extract id from document if --id is not provided", defaultValue = "id")
	private String idField;

	@Override
	public Integer call() throws Exception {

		int sourceCount = (json != null ? 1 : 0) + (file != null ? 1 : 0) + (stdin ? 1 : 0);
		if (sourceCount == 0) {
			System.err.println("One of --json, --file, or --stdin must be provided");
			return CommandLine.ExitCode.USAGE;
		}
		if (sourceCount > 1) {
			System.err.println("Only one of --json, --file, or --stdin can be provided");
			return CommandLine.ExitCode.USAGE;
		}

		String jsonContent;
		if (json != null) {
			jsonContent = json;
		}
		else if (stdin) {
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
				jsonContent = reader.lines().collect(Collectors.joining("\n"));
			}
		}
		else {
			if (!file.exists()) {
				System.err.println("File not found: " + file.getAbsolutePath());
				return CommandLine.ExitCode.SOFTWARE;
			}
			jsonContent = Files.readString(file.toPath(), StandardCharsets.UTF_8);
		}

		String index = singleIndexArgs.getIndex();

		String uniqueId = id;
		if (uniqueId == null) {
			Document doc = Document.parse(jsonContent);
			Object idValue = doc.get(idField);
			if (idValue == null) {
				System.err.println("No --id provided and field <" + idField + "> not found in document");
				return CommandLine.ExitCode.USAGE;
			}
			uniqueId = idValue.toString();
		}

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		Store store = new Store(uniqueId, index);
		store.setResultDocument(jsonContent);

		zuliaWorkPool.store(store);

		System.out.println("Stored document with id <" + uniqueId + "> in index <" + index + ">");

		return CommandLine.ExitCode.OK;
	}
}

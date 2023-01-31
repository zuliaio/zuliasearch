package io.zulia.server.cmd.common;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.message.ZuliaIndex.IndexAlias;
import picocli.CommandLine;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AliasArgs {

	@CommandLine.Option(names = { "-a", "--alias",
			"--aliases" }, paramLabel = "alias", description = "Alais name (wildcard allowed).  For multiple aliases, repeat arg or use commas to separate within a single arg", split = ",", required = true)
	private Collection<String> aliases;

	public Set<String> resolveAliases(ZuliaWorkPool pool) throws Exception {

		Set<String> resolvedAliases = new LinkedHashSet<>();
		Collection<String> allAliases = null;

		for (String index : aliases) {
			if (index.contains("*")) {
				if (allAliases == null) {
					allAliases = pool.getNodes().getIndexAliases().stream().map(IndexAlias::getAliasName).collect(Collectors.toSet());
				}
				Pattern aliasPattern = Pattern.compile(index.replace("*", ".*"));
				for (String a : allAliases) {
					if (aliasPattern.matcher(a).matches()) {
						resolvedAliases.add(a);
					}
				}
			}
			else {
				resolvedAliases.add(index);
			}

		}

		return resolvedAliases;
	}

}

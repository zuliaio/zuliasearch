package io.zulia.server.cmd.common;

import io.zulia.client.pool.ZuliaWorkPool;
import picocli.CommandLine;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class MultipleIndexArgs {

    @CommandLine.Option(names = {"-i", "--indexes",
            "--index"}, paramLabel = "index", description = "Index name (wildcard allowed).  For multiple indexes, repeat arg or use commas to separate within a single arg", split = ",", required = true)
    private Collection<String> indexes;

    public Set<String> resolveIndexes(ZuliaWorkPool pool) throws Exception {

        Set<String> resolvedIndexes = new LinkedHashSet<>();
        Collection<String> allIndexes = null;

        for (String index : indexes) {
            if (index.contains("*")) {
                if (allIndexes == null) {
                    allIndexes = pool.getIndexes().getIndexNames();
                }
                Pattern indexPattern = Pattern.compile(index.replace("*", ".*"));
                for (String i : allIndexes) {
                    if (indexPattern.matcher(i).matches()) {
                        resolvedIndexes.add(i);
                    }
                }
            } else {
                resolvedIndexes.add(index);
            }

        }

        return resolvedIndexes;
    }

}
